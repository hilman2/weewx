#
#    Copyright (c) 2009-2024 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#

"""Main engine for the weewx weather system."""

# Python imports
import gc
import logging
import math
import socket
import sys
import threading
import time
import traceback

# weewx imports:
import weedb
import weeutil.config
import weeutil.logger
import weeutil.weeutil
import weewx.accum
import weewx.loopstore
import weewx.manager
import weewx.qc
import weewx.station
import weewx.units
from weeutil.weeutil import to_bool, to_int, to_sorted_string
from weewx import all_service_groups

log = logging.getLogger(__name__)


def _same_value(was, now):
    """Whether two record values are the same number, allowing for float wobble."""
    if was is None or now is None:
        return was is None and now is None
    if isinstance(was, float) or isinstance(now, float):
        return abs(float(was) - float(now)) <= 1e-9 * max(1.0, abs(float(was)))
    return was == now


class BreakLoop(Exception):
    """Exception raised when it's time to break the main loop."""


class InitializationError(weewx.WeeWxIOError):
    """Exception raised when unable to initialize the console."""


# ==============================================================================
#                    Class StdEngine
# ==============================================================================

class StdEngine:
    """The main engine responsible for the creating and dispatching of events
    from the weather station.
    
    It loads a set of services, specified by an option in the configuration
    file.
    
    When a service loads, it binds callbacks to events. When an event occurs,
    the bound callback will be called."""

    def __init__(self, config_dict):
        """Initialize an instance of StdEngine.
        
        config_dict: The configuration dictionary. """

        # Set a default socket time out, in case FTP or HTTP hang:
        timeout = int(config_dict.get('socket_timeout', 20))
        socket.setdefaulttimeout(timeout)

        # Default garbage collection is every 3 hours:
        self.gc_interval = int(config_dict.get('gc_interval', 3 * 3600))

        # Whether to log events. This can be very verbose.
        self.log_events = to_bool(config_dict.get('log_events', False))

        # The callback dictionary:
        self.callbacks = dict()

        # This will hold an instance of the device driver
        self.console = None

        # Set up the database binder
        self.db_binder = weewx.manager.DBBinder(config_dict)

        # Set up the device driver:
        self.setupStation(config_dict)

        # Set up information about the station
        self.stn_info = weewx.station.StationInfo(self.console, **config_dict['Station'])

        # The list of instantiated services
        self.service_obj = []

        # Load the services:
        self.loadServices(config_dict)

    def setupStation(self, config_dict):
        """Set up the weather station hardware."""

        # Get the hardware type from the configuration dictionary. This will be
        # a string such as "VantagePro"
        station_type = config_dict['Station']['station_type']

        # Find the driver name for this type of hardware
        driver = config_dict[station_type]['driver']

        log.info("Loading station type %s (%s)", station_type, driver)

        # Import the driver:
        __import__(driver)

        # Open up the weather station, wrapping it in a try block in case
        # of failure.
        try:
            # This is a bit of Python wizardry. First, find the driver module
            # in sys.modules.
            driver_module = sys.modules[driver]
            # Find the function 'loader' within the module:
            loader_function = getattr(driver_module, 'loader')
            # Call it with the configuration dictionary as the only argument:
            self.console = loader_function(config_dict, self)
        except Exception as ex:
            log.error("Import of driver failed: %s (%s)", ex, type(ex))
            weeutil.logger.log_traceback(log.critical, "    ****  ")
            # Signal that we have an initialization error:
            raise InitializationError(ex)

    def loadServices(self, config_dict):
        """Set up the services to be run."""

        # Make sure all service groups are lists (if there's just a single entry, ConfigObj
        # will parse it as a string if it did not have a trailing comma).
        for service_group in config_dict['Engine']['Services']:
            if not isinstance(config_dict['Engine']['Services'][service_group], list):
                config_dict['Engine']['Services'][service_group] \
                    = [config_dict['Engine']['Services'][service_group]]

        # Versions before v4.2 did not have the service group 'xtype_services'. Set a default
        # for them:
        config_dict['Engine']['Services'].setdefault('xtype_services',
                                                     ['weewx.wxxtypes.StdWXXTypes',
                                                      'weewx.wxxtypes.StdPressureCooker',
                                                      'weewx.wxxtypes.StdRainRater',
                                                      'weewx.wxxtypes.StdDelta'])

        # Wrap the instantiation of the services in a try block, so if an
        # exception occurs, any service that may have started can be shut
        # down in an orderly way.
        try:
            # Go through each of the service lists one by one:
            for service_group in all_service_groups:
                # For each service list, retrieve all the listed services.
                # Provide a default, empty list in case the service list is
                # missing completely:
                svcs = config_dict['Engine']['Services'].get(service_group, [])
                for svc in svcs:
                    if svc == '':
                        log.debug("No services in service group %s", service_group)
                        continue
                    log.debug("Loading service %s", svc)
                    # Get the class, then instantiate it with self and the config dictionary as
                    # arguments:
                    obj = weeutil.weeutil.get_object(svc)(self, config_dict)
                    # Append it to the list of open services.
                    self.service_obj.append(obj)
                    log.debug("Finished loading service %s", svc)
        except Exception as exception:
            # An exception occurred. Shut down any running services, then
            # reraise the exception.
            self.shutDown(exception)
            raise

    def run(self):
        """Main execution entry point."""

        # Wrap the outer loop in a try block so we can do an orderly shutdown
        # should an exception occur:
        try:
            # Send out a STARTUP event:
            self.dispatchEvent(weewx.Event(weewx.STARTUP))

            log.info("Starting main packet loop.")

            last_gc = time.time()

            # This is the outer loop. 
            while True:

                # See if garbage collection is scheduled:
                if time.time() - last_gc > self.gc_interval:
                    gc_start = time.time()
                    ngc = gc.collect()
                    last_gc = time.time()
                    gc_time = last_gc - gc_start
                    log.info("Garbage collected %d objects in %.2f seconds", ngc, gc_time)

                # First, let any interested services know the packet LOOP is
                # about to start
                self.dispatchEvent(weewx.Event(weewx.PRE_LOOP))

                # Get ready to enter the main packet loop. An exception of type
                # BreakLoop will get thrown when a service wants to break the
                # loop and interact with the console.
                try:

                    # And this is the main packet LOOP. It will continuously
                    # generate LOOP packets until some service breaks it by
                    # throwing an exception (usually when an archive period
                    # has passed).
                    for packet in self.console.genLoopPackets():
                        # Package the packet as an event, then dispatch it.
                        self.dispatchEvent(weewx.Event(weewx.NEW_LOOP_PACKET, packet=packet))

                        # Allow services to break the loop by throwing
                        # an exception:
                        self.dispatchEvent(weewx.Event(weewx.CHECK_LOOP, packet=packet))

                    log.critical("Internal error. Packet loop has exited.")

                except BreakLoop:

                    # Send out an event saying the packet LOOP is done:
                    self.dispatchEvent(weewx.Event(weewx.POST_LOOP))

        # We are catching BaseException so that we can capture the exception and pass it services
        # via the SHUTDOWN event. We will then reraise it so that weewxd can handle it.
        # Although not the same as a finally block, this is the only way to capture the exception.
        except BaseException as exception:
            # The main loop has exited. Shut the engine down.
            log.info("Main loop exiting. Shutting engine down.")
            self.shutDown(exception)
            raise exception

    def bind(self, event_type, callback):
        """Binds an event to a callback function."""

        # Each event type has a list of callback functions to be called.
        # If we have not seen the event type yet, then create an empty list,
        # otherwise append to the existing list:
        self.callbacks.setdefault(event_type, []).append(callback)

    def dispatchEvent(self, event):
        """Call all registered callbacks for an event."""
        # See if any callbacks have been registered for this event type:
        if event.event_type in self.callbacks:
            if self.log_events:
                log.debug(event)
            # Yes, at least one has been registered. Call them in order:
            for callback in self.callbacks[event.event_type]:
                # Call the function with the event as an argument:
                callback(event)

    def shutDown(self, exception=None):
        """Run when an engine shutdown is requested."""

        # Send out an event saying the engine is shutting down.
        error = {}
        if exception:
            error = {
                "exception": exception,
                "stacktrace": traceback.format_exc()
            }
        try:
            self.dispatchEvent(weewx.Event(weewx.SHUTDOWN, error=error))
        except:
            pass

        # Shut down all the services
        while self.service_obj:
            # Wrap each individual service shutdown, in case of a problem.
            try:
                # Start from the end of the list and move forward
                self.service_obj[-1].shutDown()
            except:
                pass
            # Delete the actual service
            del self.service_obj[-1]

        try:
            # Close the console:
            self.console.closePort()
        except:
            pass

        try:
            self.db_binder.close()
        except:
            pass

    def _get_console_time(self):
        try:
            return self.console.getTime()
        except NotImplementedError:
            return int(time.time() + 0.5)


# ==============================================================================
#                    Class DummyEngine
# ==============================================================================

class DummyEngine(StdEngine):
    """A dummy engine, useful for loading services, but without actually running the engine."""

    class DummyConsole:
        """A dummy console, used to offer an archive_interval."""

        def __init__(self, config_dict):
            try:
                self.archive_interval = to_int(config_dict['StdArchive']['archive_interval'])
            except KeyError:
                self.archive_interval = 300

        def closePort(self):
            pass

    def setupStation(self, config_dict):
        self.console = DummyEngine.DummyConsole(config_dict)


# ==============================================================================
#                    Class StdService
# ==============================================================================

class StdService:
    """Abstract base class for all services."""

    def __init__(self, engine, config_dict):
        self.engine = engine
        self.config_dict = config_dict

    def bind(self, event_type, callback):
        """Bind the specified event to a callback."""
        # Just forward the request to the main engine:
        self.engine.bind(event_type, callback)

    def shutDown(self):
        pass


# ==============================================================================
#                    Class StdConvert
# ==============================================================================

class StdConvert(StdService):
    """Service for performing unit conversions.
    
    This service acts as a filter. Whatever packets and records come in are
    converted to a target unit system.
    
    This service should be run before most of the others, so observations appear
    in the correct unit."""

    def __init__(self, engine, config_dict):
        # Initialize my base class:
        super().__init__(engine, config_dict)

        # Get the target unit nickname (something like 'US' or 'METRIC'). If there is no
        # target, then do nothing
        try:
            target_unit_nickname = config_dict['StdConvert']['target_unit']
        except KeyError:
            # Missing target unit.
            return
        # Get the target unit: weewx.US, weewx.METRIC, weewx.METRICWX
        self.target_unit = weewx.units.unit_constants[target_unit_nickname.upper()]
        # Bind self.converter to the appropriate standard converter
        self.converter = weewx.units.StdUnitConverters[self.target_unit]

        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        log.info("StdConvert target unit is 0x%x", self.target_unit)

    def new_loop_packet(self, event):
        """Do unit conversions for a LOOP packet"""
        # No need to do anything if the packet is already in the target
        # unit system
        if event.packet['usUnits'] == self.target_unit:
            return
        # Perform the conversion
        converted_packet = self.converter.convertDict(event.packet)
        # Add the new unit system
        converted_packet['usUnits'] = self.target_unit
        # Replace the old packet with the new, converted packet:
        event.packet = converted_packet

    def new_archive_record(self, event):
        """Do unit conversions for an archive record."""
        # No need to do anything if the record is already in the target
        # unit system
        if event.record['usUnits'] == self.target_unit:
            return
        # Perform the conversion
        converted_record = self.converter.convertDict(event.record)
        # Add the new unit system
        converted_record['usUnits'] = self.target_unit
        # Replace the old record with the new, converted record
        event.record = converted_record


# ==============================================================================
#                    Class StdCalibrate
# ==============================================================================

class StdCalibrate(StdService):
    """Adjust data using calibration expressions.
    
    This service must be run before StdArchive, so the correction is applied
    before the data is archived.

    To use:
    [StdWXCalibrate]
      [[Corrections]]
        obstype = expression[, loop][, archive]

    where "expression" is a valid Python expression involving any observation types in the same
    record, or functions in the "math" module. If only "loop" is present, then only do the
    correction in LOOP packets. If only "archive" is present, then only do the corrections in the
    archive records. If neither is present, always do the correction in LOOP packets, and do it in
    archive records if software record generation is being done.
    """

    def __init__(self, engine, config_dict):
        # Initialize my base class:
        super().__init__(engine, config_dict)

        if 'StdCalibrate' not in config_dict or 'Corrections' not in config_dict['StdCalibrate']:
            log.info("No calibration information in config file. Ignored.")
            return

        # Get the set of calibration corrections to apply.
        correction_dict = config_dict['StdCalibrate']['Corrections']
        self.expressions = {}
        self.corrections = {}
        self.which = {}
        for obs_type in correction_dict.scalars:
            if obs_type == 'foo': continue
            # Ensure that the value is always a list
            option = weeutil.weeutil.option_as_list(correction_dict[obs_type])
            # Save the uncompiled expression
            self.expressions[obs_type] = option[0]
            # For each correction, compile it, then save in a dictionary of
            # corrections to be applied:
            self.corrections[obs_type] = compile(option[0], 'StdCalibrate', 'eval')
            # Now save which types of packets/records it should apply to. This will be either
            # an empty list, or a list of one or two elements, which can be the strings 'loop',
            # or 'archive', or both.
            self.which[obs_type] = [v.lower() for v in option[1:]]
            # Check the directives:
            for val in self.which[obs_type]:
                if val not in ('loop', 'archive'):
                    raise ValueError(f"Invalid directive for StdCalibrate: {val}")

        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_loop_packet(self, event):
        """Apply a calibration correction to a LOOP packet"""
        for obs_type in self.corrections:
            # If no directives were specified (self.which is empty), then always do the correction.
            # If a directive has been specified, do the correction if 'loop' is in the directive.
            if len(self.which[obs_type]) == 0 or 'loop' in self.which[obs_type]:
                try:
                    event.packet[obs_type] = eval(self.corrections[obs_type], {'math': math},
                                                  event.packet)
                except (TypeError, NameError) as e:
                    if weewx.debug >= 2:
                        log.debug("StdCalibrate type or name error in LOOP packet: %s", e)
                except ValueError as e:
                    log.error("StdCalibrate value error in LOOP packet %s", e)

    def new_archive_record(self, event):
        """Apply a calibration correction to an archive packet"""
        for obs_type in self.corrections:
            # If a record was software-generated, then the correction has presumably been
            # already applied in the LOOP packet. So, unless told otherwise, do not do the
            # correction again.
            if ((len(self.which[obs_type]) == 0 and event.origin != 'software')
                    or 'archive' in self.which[obs_type]):
                try:
                    event.record[obs_type] = eval(self.corrections[obs_type], {'math': math},
                                                  event.record)
                except (TypeError, NameError) as e:
                    if weewx.debug >= 2:
                        log.debug("StdCalibrate type or name error in archive record: %s", e)
                except ValueError as e:
                    log.error("StdCalibrate value error in archive record: %s", e)


# ==============================================================================
#                    Class StdQC
# ==============================================================================

class StdQC(StdService):
    """Service that performs quality check on incoming data.

    A StdService wrapper for a QC object, so it may be called as a service. This
    also allows the weewx.qc.QC class to be used elsewhere without the 
    overheads of running it as a weewx service.
    """

    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)

        # If the 'StdQC' or 'MinMax' sections do not exist in the configuration
        # dictionary, then an exception will get thrown and nothing will be
        # done.
        try:
            mm_dict = config_dict['StdQC']['MinMax']
        except KeyError:
            log.info("No QC information in config file.")
            return
        log_failure = to_bool(weeutil.config.search_up(config_dict['StdQC'],
                                                       'log_failure', True))

        self.qc = weewx.qc.QC(mm_dict, log_failure)

        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_loop_packet(self, event):
        """Apply quality check to the data in a loop packet"""

        self.qc.apply_qc(event.packet, 'LOOP')

    def new_archive_record(self, event):
        """Apply quality check to the data in an archive record"""

        self.qc.apply_qc(event.record, 'Archive')


# ==============================================================================
#                    Class StdArchive
# ==============================================================================

class StdArchive(StdService):
    """Service that archives LOOP and archive data in the SQL databases."""

    # This service manages an "accumulator", which records high/lows and
    # averages of LOOP packets over an archive period. At the end of the
    # archive period it then emits an archive record.

    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)

        # Extract the various options from the config file. If it's missing, fill in with defaults:
        archive_dict = config_dict.get('StdArchive', {})
        self.data_binding = archive_dict.get('data_binding', 'wx_binding')
        self.record_generation = archive_dict.get('record_generation', 'hardware').lower()
        self.no_catchup = to_bool(archive_dict.get('no_catchup', False))
        self.archive_delay = to_int(archive_dict.get('archive_delay', 15))
        software_interval = to_int(archive_dict.get('archive_interval', 300))
        self.loop_hilo = to_bool(archive_dict.get('loop_hilo', True))
        self.record_augmentation = to_bool(archive_dict.get('record_augmentation', True))
        self.log_success = to_bool(weeutil.config.search_up(archive_dict, 'log_success', True))
        self.log_failure = to_bool(weeutil.config.search_up(archive_dict, 'log_failure', True))

        log.info("Archive will use data binding %s", self.data_binding)
        log.info("Record generation will be attempted in '%s'", self.record_generation)

        # The timestamp that marks the end of the archive period
        self.end_archive_period_ts = None
        # The timestamp that marks the end of the archive period, plus a delay
        self.end_archive_delay_ts = None
        # Set while a record is being worked out, so that new_archive_record can
        # augment it from the same packets. None the rest of the time.
        self.old_accumulator = None
        # The newest packet timestamp seen. What "now" means for deciding which
        # periods have closed, so that a replay behaves like live data.
        self.latest_ts = None
        # Periods that received a packet and have not been worked out yet.
        self.touched = set()
        # Days whose summaries need building again, because a record in them was
        # written a second time. Collected so that a block of late data costs one
        # rebuild per day rather than one per record.
        self.days_to_rebuild = set()
        self.last_trim = 0

        if self.record_generation == 'software':
            self.archive_interval = software_interval
            ival_msg = "(software record generation)"
        elif self.record_generation == 'hardware':
            # If the station supports a hardware archive interval, use that.
            # Warn if it is different from what is in config.
            try:
                if software_interval != self.engine.console.archive_interval:
                    log.info("The archive interval in the configuration file (%d) does not "
                             "match the station hardware interval (%d).",
                             software_interval,
                             self.engine.console.archive_interval)
                self.archive_interval = self.engine.console.archive_interval
                ival_msg = "(specified by hardware)"
            except NotImplementedError:
                self.archive_interval = software_interval
                ival_msg = "(specified in weewx configuration)"
        else:
            log.error("Unknown type of record generation: %s", self.record_generation)
            raise ValueError(self.record_generation)

        log.info("Using archive interval of %d seconds %s", self.archive_interval, ival_msg)

        if self.archive_delay <= 0:
            raise weewx.ViolatedPrecondition("Archive delay (%.1f) must be greater than zero."
                                             % (self.archive_delay,))
        if self.archive_delay >= self.archive_interval / 2:
            log.warning("Archive delay (%d) is unusually long", self.archive_delay)

        log.debug("Use LOOP data in hi/low calculations: %d", self.loop_hilo)

        weewx.accum.initialize(config_dict)

        # Every LOOP packet goes into the ingest table, and every archive record is
        # worked out from it. Keeping the packets is what lets a reading that arrives
        # after its period has closed still reach the record it belongs to, and what
        # keeps a restart from losing the period it happened in.
        self.retain_days = to_int(archive_dict.get('retain_days',
                                                   weewx.loopstore.DEFAULT_RETAIN_DAYS))
        self.archive_days = to_int(archive_dict.get('archive_days',
                                                    weewx.loopstore.DEFAULT_ARCHIVE_DAYS))
        # Which field of a packet says where it came from. Recorded for diagnosis when
        # several sources report to one WeeWX; nothing is decided by it.
        self.source_field = archive_dict.get('source_field', 'station')
        # Whether the console keeps an archive of its own. Taken from the
        # configuration, and corrected the first time the console says it does not.
        # Plenty of hardware is configured for it and cannot do it: a station may have
        # a logger and still not offer it over the protocol it uploads with.
        self.hardware_archive = (self.record_generation == 'hardware')
        try:
            self.store = weewx.loopstore.LoopStore.open_with_config(
                config_dict,
                archive_dict.get('ingest_binding', weewx.loopstore.DEFAULT_BINDING),
                self.data_binding,
                archive_dict.get('archive_dir') or None)
        except (weedb.DatabaseError, OSError) as e:
            # Every archive record is worked out from this table, so there is nothing
            # to carry on with. Say what is wrong rather than fail somewhere later.
            raise InitializationError(
                "Cannot open the ingest table, which is where LOOP packets are kept "
                "and every archive record is worked out from: %s" % e)
        self.last_trim = to_int(
            self.store.get_metadata(weewx.loopstore.LAST_TRIM, 0)) or time.time()

        self.bind(weewx.STARTUP, self.startup)
        self.bind(weewx.PRE_LOOP, self.pre_loop)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.CHECK_LOOP, self.check_loop)
        self.bind(weewx.POST_LOOP, self.post_loop)
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def startup(self, _unused):
        """Called when the engine is starting up. Main task is to set up the database, backfill it,
        then perform a catch-up if the hardware supports it. """

        # This will create the database if it doesn't exist:
        dbmanager = self.engine.db_binder.get_manager(self.data_binding, initialize=True)
        log.info("Using binding '%s' to database '%s'", self.data_binding, dbmanager.database_name)

        # Make sure the daily summaries have not been partially updated
        if dbmanager._read_metadata('lastWeightPatch'):
            raise weewx.ViolatedPrecondition("Update of daily summary for database '%s' not"
                                             " complete. Finish the update first."
                                             % dbmanager.database_name)

        # Backfill the daily summaries.
        _nrecs, _ndays = dbmanager.backfill_day_summary()

        # Periods that received packets but were never worked out, because the last
        # run stopped between the two. Without this a restart in the middle of a
        # period loses that period, which is one of the things the ingest table is
        # for.
        self._pick_up_where_we_left_off()

        # Do a catch-up on any data still on the station, but not yet put in the database.
        if self.no_catchup:
            log.debug("No catchup specified.")
            # The catchup below is also where we find out whether the console has an
            # archive at all. Without it, ask in a way that changes nothing.
            self._settle_hardware_archive()
        else:
            # Not all consoles can do a hardware catchup, so be prepared to catch the exception:
            try:
                self._catchup(self.engine.console.genStartupRecords)
            except NotImplementedError:
                # And this is where we find out. Without it the flag would stay as the
                # configuration left it until a record happens to be written from
                # scratch, and until then no record would ever be put right.
                if self.hardware_archive:
                    log.info("The console has no archive of its own, so records are "
                             "worked out from the LOOP packets.")
                self.hardware_archive = False

    def pre_loop(self, _event):
        """Called before the main packet loop is entered."""

        # If this is the initial time through the loop, then the end of
        # the archive and delay periods need to be primed:
        if not self.end_archive_period_ts:
            now = self.engine._get_console_time()
            start_archive_period_ts = weeutil.weeutil.startOfInterval(now, self.archive_interval)
            self.end_archive_period_ts = start_archive_period_ts + self.archive_interval
            self.end_archive_delay_ts = self.end_archive_period_ts + self.archive_delay
        self.old_accumulator = None

    def new_loop_packet(self, event):
        """Called when a new LOOP record has arrived.

        The packet goes into the ingest table, and that is the whole of it. Nothing is
        accumulated in memory, so nothing is lost to a restart, and a packet whose
        timestamp belongs to an earlier period simply lands in that period.
        """
        timestamp = event.packet['dateTime']
        self.latest_ts = timestamp if self.latest_ts is None \
            else max(self.latest_ts, timestamp)
        try:
            self.store.add(event.packet, source=event.packet.get(self.source_field))
        except (weedb.DatabaseError, TypeError, ValueError) as e:
            log.error("Cannot store the LOOP packet from %s: %s",
                      weeutil.weeutil.timestamp_to_string(timestamp), e)
            return
        self.touched.add(weeutil.weeutil.startOfInterval(timestamp,
                                                         self.archive_interval))

    def check_loop(self, event):
        """Called after any loop packets have been processed. This is the opportunity
        to break the main loop by throwing an exception."""
        # Is this the end of the archive period? If so, dispatch an
        # END_ARCHIVE_PERIOD event
        if event.packet['dateTime'] > self.end_archive_period_ts:
            self.engine.dispatchEvent(weewx.Event(weewx.END_ARCHIVE_PERIOD,
                                                  packet=event.packet,
                                                  end=self.end_archive_period_ts))
            start_archive_period_ts = weeutil.weeutil.startOfInterval(event.packet['dateTime'],
                                                                      self.archive_interval)
            self.end_archive_period_ts = start_archive_period_ts + self.archive_interval

        # Has the end of the archive delay period ended? If so, break the loop.
        if event.packet['dateTime'] >= self.end_archive_delay_ts:
            raise BreakLoop

    def post_loop(self, _event):
        """The main packet loop has ended, so work out the periods that have closed.

        A record is what the packets of its period add up to, so every period that
        received something and has since ended is worked out from the ingest table.
        Where a record for it is already in the database and the packets no longer
        agree with it, which is what a late packet does, it is written again.
        """
        done = self._finished_periods()
        for start in done:
            try:
                self._emit(start)
            except (weedb.DatabaseError, weewx.accum.OutOfSpan) as e:
                log.error("Could not work out the record for %s: %s",
                          weeutil.weeutil.timestamp_to_string(start
                                                              + self.archive_interval),
                          e)
        if self.days_to_rebuild:
            # Once per day touched, however many of its records were written again.
            dbmanager = self._dbmanager()
            for day_ts in sorted(self.days_to_rebuild):
                weewx.loopstore.rebuild_day(dbmanager, day_ts + 1, self.store,
                                            self.archive_interval, self.loop_hilo)
            self.days_to_rebuild.clear()

        if done:
            # Everything up to the end of the last period dealt with is accounted for.
            # Later packets belong to a period still open and keep their place in the
            # queue, so that a restart picks them up rather than going over old ground.
            try:
                self.store.set_metadata(
                    weewx.loopstore.APPLIED_THROUGH,
                    self.store.max_seq_through(done[-1] + self.archive_interval))
            except weedb.DatabaseError as e:
                log.error("Could not record how far the archive has been worked "
                          "out: %s", e)
        self._trim()

        # Set the time of the next break loop:
        self.end_archive_delay_ts = self.end_archive_period_ts + self.archive_delay

    def _settle_hardware_archive(self):
        """Find out whether the console has an archive of its own, without using it.

        Asking for records since the newest one in the database yields nothing on a
        console that is up to date, and NotImplementedError on one that has no archive.
        Either answer settles it; neither writes anything.
        """
        if not self.hardware_archive:
            return
        try:
            generator = self.engine.console.genArchiveRecords(self._dbmanager()
                                                              .lastGoodStamp())
            next(iter(generator), None)
        except NotImplementedError:
            log.info("The console has no archive of its own, so records are worked "
                     "out from the LOOP packets.")
            self.hardware_archive = False
        except Exception as e:                                       # noqa: BLE001
            # Anything else means the console does have an archive and simply could
            # not be read just now. Leave the flag alone.
            log.debug("Could not ask the console for records: %s", e)

    def _pick_up_where_we_left_off(self):
        """Queue up the periods whose packets have not been through post_loop yet."""
        try:
            applied = to_int(self.store.get_metadata(weewx.loopstore.APPLIED_THROUGH, 0))
            times = self.store.times_after(applied)
        except weedb.DatabaseError as e:
            log.error("Could not read the ingest table: %s", e)
            return
        if not times:
            return
        self.latest_ts = max(times) if self.latest_ts is None             else max(self.latest_ts, max(times))
        self.touched.update(weeutil.weeutil.startOfInterval(ts, self.archive_interval)
                            for ts in times)
        log.info("%d packet(s) in the ingest table have not been archived yet.",
                 len(times))

    def _finished_periods(self):
        """Periods that received a packet and have since ended, oldest first."""
        if self.latest_ts is None:
            return []
        due = sorted(start for start in self.touched
                     if start + self.archive_interval <= self.latest_ts)
        self.touched.difference_update(due)
        return due

    def _emit(self, start):
        """Turn one period into an archive record, or put an existing one right."""
        stop = start + self.archive_interval
        self.old_accumulator = self._accumulator_for(start, stop)
        if self.old_accumulator is None:
            return
        try:
            existing = self._dbmanager().getRecord(stop)
            if existing is not None and self.hardware_archive:
                # The console keeps its own archive and is the authority for its own
                # periods. A late packet does not overrule what it measured.
                return
            if existing is not None:
                # A record for this time is already in the database. Only a packet
                # that arrived after it was written can have brought us back here.
                if not self._held_whole(start):
                    # And only part of the period is still held, the rest having been
                    # trimmed away. Working it out from what survived would replace a
                    # record made from a hundred readings with one made from a handful.
                    log.warning("Archive record %s has late packets, but the rest of "
                                "its period is no longer held. Leaving it alone. "
                                "Raise 'archive_days', or use 'weectl import "
                                "--update' for data that old.",
                                weeutil.weeutil.timestamp_to_string(stop))
                    return
                if self._revise(existing, stop):
                    # The day this record falls in has to be built again, but not
                    # here: a block of late data puts a dozen records right at once,
                    # and each rebuild reads the whole day.
                    self.days_to_rebuild.add(weeutil.weeutil.startOfArchiveDay(stop))
                return

            # If the user has requested software generation, then do that:
            if self.record_generation == 'software':
                self._software_catchup()
            elif self.record_generation == 'hardware':
                # Otherwise, try to honor hardware generation. An exception
                # will be raised if the console does not support it. In that
                # case, fall back to software generation.
                try:
                    self._catchup(self.engine.console.genArchiveRecords)
                except NotImplementedError:
                    # No archive on the console after all. Remember that, so a
                    # record already there is put right from the packets rather
                    # than written a second time.
                    self.hardware_archive = False
                    self._software_catchup()
            else:
                raise ValueError("Unknown station record generation value %s"
                                 % self.record_generation)
        finally:
            self.old_accumulator = None

    def _revise(self, existing, stop):
        """Write a record again because its packets no longer agree with it.

        Returns True if anything changed.
        """
        dbmanager = self._dbmanager()
        record = self.old_accumulator.getRecord()
        record['interval'] = self.archive_interval / 60
        differing = sorted(k for k, v in record.items()
                           if k in dbmanager.sqlkeys
                           and k not in ('dateTime', 'usUnits', 'interval')
                           and not _same_value(existing.get(k), v))
        if not differing:
            return False
        log.info("Record %s no longer agrees with its packets on %s. Writing it again.",
                 weeutil.weeutil.timestamp_to_string(stop), ', '.join(differing))
        with weewx.loopstore.keeping_last_update(dbmanager):
            dbmanager.addRecord(record, update=True)
        return True

    def _held_whole(self, start):
        """Whether the ingest table still has the whole of a period.

        Inside the retention it does. Beyond that only if the day was written out to
        a file and that file is still there.
        """
        now = time.time()
        if start >= now - self.retain_days * 86400:
            return True
        if self.archive_days and start < now - self.archive_days * 86400:
            return False
        return self.store.has_day(start)

    def _accumulator_for(self, start, stop):
        """The packets of a period, accumulated, or None if there are none."""
        accumulator = weewx.accum.Accum(weeutil.weeutil.TimeSpan(start, stop))
        for packet in self.store.packets(start, stop):
            try:
                accumulator.addRecord(packet, add_hilo=self.loop_hilo)
            except (weewx.accum.OutOfSpan, ValueError) as e:
                log.debug("Ignoring a stored packet from %s: %s",
                          weeutil.weeutil.timestamp_to_string(packet.get('dateTime')), e)
        return None if accumulator.isEmpty else accumulator

    def _dbmanager(self):
        return self.engine.db_binder.get_manager(self.data_binding)

    def _trim(self):
        """Move packets out of the ingest table once a day."""
        now = time.time()
        if now - self.last_trim < 86400:
            return
        self.last_trim = now
        try:
            self.store.set_metadata(weewx.loopstore.LAST_TRIM, int(now))
            gone = self.store.trim(now - self.retain_days * 86400)
            if gone:
                log.info("Moved %d packet(s) older than %d days out of the ingest "
                         "table.", gone, self.retain_days)
            if self.archive_days:
                dropped = self.store.trim_archive(now - self.archive_days * 86400)
                if dropped:
                    log.info("Deleted %d day file(s) older than %d days.",
                             dropped, self.archive_days)
        except weedb.DatabaseError as e:
            log.error("Could not trim the ingest table: %s", e)

    def new_archive_record(self, event):
        """Called when a new archive record has arrived.
        Put it in the archive database."""

        # If requested, extract any extra information we can out of the accumulator and put it in
        # the record. Not necessary in the case of software record generation because it has
        # already been done.
        if self.record_augmentation \
                and self.old_accumulator \
                and event.record['dateTime'] == self.old_accumulator.timespan.stop \
                and event.origin != 'software':
            self.old_accumulator.augmentRecord(event.record)

        dbmanager = self.engine.db_binder.get_manager(self.data_binding)
        dbmanager.addRecord(event.record,
                            accumulator=self.old_accumulator,
                            log_success=self.log_success,
                            log_failure=self.log_failure)

    def _catchup(self, generator):
        """Pull any unarchived records off the console and archive them.
        
        If the hardware does not support hardware archives, an exception of
        type NotImplementedError will be thrown."""

        dbmanager = self.engine.db_binder.get_manager(self.data_binding)
        # Find out when the database was last updated.
        lastgood_ts = dbmanager.lastGoodStamp()

        try:
            # Now ask the console for any new records since then. Not all
            # consoles support this feature. Note that for some consoles,
            # notably the Vantage, when doing a long catchup the archive
            # records may not be on the same boundaries as the archive
            # interval. Reject any records that have a timestamp in the
            # future, but provide some lenience for clock drift.
            for record in generator(lastgood_ts):
                ts = record.get('dateTime')
                if ts and ts < time.time() + self.archive_delay:
                    self.engine.dispatchEvent(weewx.Event(weewx.NEW_ARCHIVE_RECORD,
                                                          record=record,
                                                          origin='hardware'))
                else:
                    log.warning("Ignore historical record: %s" % record)
        except weewx.HardwareError as e:
            log.error("Internal error detected. Catchup abandoned")
            log.error("**** %s" % e)

    def shutDown(self):
        if self.store:
            self.store.close()
            self.store = None

    def _software_catchup(self):
        # Extract a record out of the old accumulator. 
        record = self.old_accumulator.getRecord()
        # Add the archive interval
        record['interval'] = self.archive_interval / 60
        # Send out an event with the new record:
        self.engine.dispatchEvent(weewx.Event(weewx.NEW_ARCHIVE_RECORD,
                                              record=record,
                                              origin='software'))

    def _new_accumulator(self, timestamp):
        start_ts = weeutil.weeutil.startOfInterval(timestamp,
                                                   self.archive_interval)
        end_ts = start_ts + self.archive_interval

        # Instantiate a new accumulator
        new_accumulator = weewx.accum.Accum(weeutil.weeutil.TimeSpan(start_ts, end_ts))
        return new_accumulator


# ==============================================================================
#                    Class StdTimeSynch
# ==============================================================================

class StdTimeSynch(StdService):
    """Regularly asks the station to synch up its clock."""

    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)

        # Zero out the time of last synch, and get the time between synchs.
        self.last_synch_ts = 0
        self.clock_check = int(config_dict.get('StdTimeSynch',
                                               {'clock_check': 14400}).get('clock_check', 14400))
        self.max_drift = int(config_dict.get('StdTimeSynch',
                                             {'max_drift': 5}).get('max_drift', 5))

        self.bind(weewx.STARTUP, self.startup)
        self.bind(weewx.PRE_LOOP, self.pre_loop)

    def startup(self, _event):
        """Called when the engine is starting up."""
        self.do_sync()

    def pre_loop(self, _event):
        """Called before the main event loop is started."""
        self.do_sync()

    def do_sync(self):
        """Ask the station to synch up if enough time has passed."""
        # Synch up the station's clock if it's been more than clock_check
        # seconds since the last check:
        now_ts = time.time()
        if now_ts - self.last_synch_ts >= self.clock_check:
            self.last_synch_ts = now_ts
            try:
                console_time = self.engine.console.getTime()
                if console_time is None:
                    return
                # getTime can take a long time to run, so we use the current
                # system time
                diff = console_time - time.time()
                log.info("Clock error is %.2f seconds (positive is fast)", diff)
                if abs(diff) > self.max_drift:
                    try:
                        self.engine.console.setTime()
                    except NotImplementedError:
                        log.debug("Station does not support setting the time")
            except NotImplementedError:
                log.debug("Station does not support reading the time")
            except weewx.WeeWxIOError as e:
                log.info("Error reading time: %s" % e)


# ==============================================================================
#                    Class StdPrint
# ==============================================================================

class StdPrint(StdService):
    """Service that prints diagnostic information when a LOOP
    or archive packet is received."""

    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)

        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_loop_packet(self, event):
        """Print out the new LOOP packet"""
        print("LOOP:  ",
              weeutil.weeutil.timestamp_to_string(event.packet['dateTime']),
              to_sorted_string(event.packet))

    def new_archive_record(self, event):
        """Print out the new archive record."""
        print("REC:   ",
              weeutil.weeutil.timestamp_to_string(event.record['dateTime']),
              to_sorted_string(event.record))


# ==============================================================================
#                    Class StdReport
# ==============================================================================

class StdReport(StdService):
    """Launches a separate thread to do reporting."""

    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)
        self.max_wait = int(config_dict['StdReport'].get('max_wait', 600))
        self.thread = None
        self.launch_time = None
        self.record = None
        self.stop_event = threading.Event()

        # check if pyephem is installed and make a suitable log entry
        try:
            import ephem
            log.info("'pyephem' detected, extended almanac data is available")
            del ephem
        except ImportError:
            log.info("'pyephem' not detected, extended almanac data is not available")

        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        self.bind(weewx.POST_LOOP, self.launch_report_thread)

    def new_archive_record(self, event):
        """Cache the archive record to pass to the report thread."""
        self.record = event.record

    def launch_report_thread(self, _event):
        """Called after the packet LOOP. Processes any new data."""
        import weewx.reportengine
        # Do not launch the reporting thread if an old one is still alive.
        # To guard against a zombie thread (alive, but doing nothing) launch
        # anyway if enough time has passed.
        if self.thread and self.thread.is_alive():
            thread_age = time.time() - self.launch_time
            if thread_age < self.max_wait:
                log.info("Launch of report thread aborted: existing report thread still running")
                return
            else:
                log.warning("Previous report thread has been running"
                            " %s seconds.  Launching report thread anyway.", thread_age)

        try:
            self.thread = weewx.reportengine.StdReportEngine(self.config_dict,
                                                             self.engine.stn_info,
                                                             self.record,
                                                             first_run=not self.launch_time,
                                                             stop_event=self.stop_event)
            self.thread.start()
            self.launch_time = time.time()
        except threading.ThreadError:
            log.error("Unable to launch report thread.")
            self.thread = None

    def shutDown(self):
        if self.thread:
            log.info("Shutting down StdReport thread")
            # Ask the thread to stop
            self.stop_event.set()
            # Wait up to 20 seconds, clean up, then return
            self.thread.join(20.0)
            if self.thread.is_alive():
                log.error("Unable to shut down StdReport thread")
            else:
                log.debug("StdReport thread has been terminated")
        self.thread = None
        self.launch_time = None
        self.stop_event = None

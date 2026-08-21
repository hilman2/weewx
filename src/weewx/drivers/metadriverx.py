#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Meta driver for running multiple WeeWX station drivers concurrently."""

import importlib
import logging
import queue
import threading

import weeutil.weeutil
import weewx
import weewx.drivers

DRIVER_NAME = 'MetaDriver'
DRIVER_VERSION = '1.0'

log = logging.getLogger(__name__)


def loader(config_dict, engine):
    return MetaDriver(config_dict, engine)


class ChildInfo:
    def __init__(self, station_type, thread):
        self.station_type = station_type
        self.thread = thread
        self.done_event = threading.Event()


class MetaDriver(weewx.drivers.AbstractDevice):
    """Present several station drivers as one engine-facing device."""

    _JOIN_TIMEOUT = 5.0

    def __init__(self, config_dict, engine):
        self.station_types = weeutil.weeutil.option_as_list(
            config_dict[DRIVER_NAME].get('station_types'))
        if not self.station_types:
            raise ValueError("No station_types specified")
        self.children = []
        self.task_queue = queue.SimpleQueue()
        self.stop_event = threading.Event()

        # For each station type, instantiate a driver and a thread.
        for station_type in self.station_types:
            try:
                station_config = config_dict[station_type]
            except (KeyError, TypeError):
                raise weewx.ViolatedPrecondition("MetaDriver station stanza '%s' was not found"
                                                 % station_type)

            driver_module_path = station_config['driver']

            try:
                driver_module = importlib.import_module(driver_module_path)
                loader_function = getattr(driver_module, 'loader')
                # TODO: We should be careful about giving child threads access to the engine.
                driver = loader_function(config_dict, engine)
            except Exception:
                log.exception("Unable to initialize child station '%s' (%s)",
                              station_type, driver_module_path)
                continue

            thread = WorkerThread(driver, self.task_queue)
            thread.daemon = True
            thread.name = "Worker-" + station_type
            self.children.append(ChildInfo(station_type, thread))
            thread.start()

    @property
    def hardware_name(self):
        result_queue = queue.Queue()
        self.children[0].thread.task_queue.put(('hardware_name', result_queue, (), {}))
        result = result_queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    @property
    def archive_interval(self):
        result_queue = queue.Queue()
        self.children[0].thread.task_queue.put(('archive_interval', result_queue, (), {}))
        result = result_queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    def genArchiveRecords(self, lastgood_ts):
        result_queue = queue.SimpleQueue()
        for child in self.children:
            child.thread.task_queue.put(('genArchiveRecords', result_queue, (lastgood_ts,), {}))
        while True:
            result = result_queue.get()
            if result is None:
                break
            elif isinstance(result, Exception):
                raise result
            yield result

    def genLoopPackets(self):
        result_queue = queue.SimpleQueue()
        for child in self.children:
            child.thread.task_queue.put(('genLoopPackets', result_queue, (), {}))
        while True:
            result = result_queue.get()
            if isinstance(result, Exception):
                raise result
            yield result


    def getTime(self):
        result_queue = queue.Queue()
        self.children[0].thread.task_queue.put(('getTime', result_queue, (), {}))
        result = result_queue.get()
        if isinstance(result, Exception):
            raise result
        return result

    def closePort(self):
        for child in self.children:
            child.task_queue.put((None, None, None, None))


class WorkerThread(threading.Thread):

    def __init__(self, driver, task_queue):
        super().__init__()
        self.driver = driver
        self.task_queue = task_queue

    def run(self):
        while True:
            # Get the function and its arguments from the queue
            func_name, result_queue, args, kwargs = self.task_queue.get()
            print("Worker thread got function", func_name, flush=True)

            if func_name is None:  # Sentinel to shut down
                self.driver.shutDown()
                break

            try:
                if func_name == 'hardware_name':
                    result = self.driver.hardware_name
                    result_queue.put(result)
                elif func_name == 'archive_interval':
                    result = self.driver.archive_interval
                    result_queue.put(result)
                elif func_name == 'genArchiveRecords':
                    for record in self.driver.genArchiveRecords(*args, **kwargs):
                        result_queue.put(record)
                    print("Putting None in genArchiveRecords result queue")
                    result_queue.put(None)
                elif func_name == 'genLoopPackets':
                    for packet in self.driver.genLoopPackets():
                        result_queue.put(packet)
                    # No reason we should get here
                    log.critical("Unexpected exit with task genLoopPackets")
                    result_queue.put(RuntimeError("Unexpected exit with task genLoopPackets"))
                elif func_name == 'getTime':
                    result = self.driver.getTime()
                    result_queue.put(result)
            except Exception as e:
                result_queue.put(e)

#
#    Copyright (c) 2026 Manuel Hilgert
#
#    See the file LICENSE.txt for your full rights.
#
"""Keep the LOOP packets, so that an archive record can be worked out again.

WeeWX turns LOOP packets into an archive record once, in `StdArchive`, and then the
packets are gone. The record is not a view of anything; it is the only copy. That is
why a reading which turns up after its archive period has ended cannot be put where it
belongs, and why a restart in the middle of a period loses that period.

This service keeps every LOOP packet in a table of its own for a few days. After
`StdArchive` has had its turn, it looks at the periods that received packets and
compares what the packets say with what is in the archive:

  * No record for a period that has packets? Write one.
  * A record with empty fields the packets can fill? Fill them.

Nothing is ever overwritten. A field that already holds a value keeps it, so a record
can only gain data, never change it. That covers what people run into: a second
console whose readings arrive a minute late, a sensor relayed through a service, a
restart between two archive records. It deliberately does not cover a late packet that
would change an average already in the database.

The store lives in a database of its own, so that its size and its retention have
nothing to do with the archive, and a backup can leave it out.
"""

import contextlib
import datetime
import json
import logging
import time

import weedb
import weeutil.weeutil
import weewx
import weewx.accum
import weewx.engine
import weewx.manager
from weeutil.weeutil import timestamp_to_string, to_bool, to_int

log = logging.getLogger(__name__)

DEFAULT_BINDING = 'loop_binding'
DEFAULT_RETAIN_DAYS = 7
# How much of the store to look over at startup, in days. Whatever went missing while
# weewx was not running is found there.
DEFAULT_CATCHUP_DAYS = 2
METADATA_TABLE = 'loop_metadata'
# When the store was last trimmed, so that it survives a restart.
LAST_TRIM = 'lastTrim'

# Everything a packet carries goes into 'data' as JSON, so that a new sensor needs no
# schema change. 'seq' is the order of arrival, which is what tells 'first' and 'last'
# accumulators apart when two packets share a timestamp.
table = [('seq', 'INTEGER NOT NULL PRIMARY KEY'),
         ('dateTime', 'INTEGER NOT NULL'),
         ('usUnits', 'INTEGER NOT NULL'),
         ('source', 'VARCHAR(64)'),
         ('data', 'TEXT NOT NULL'),
         ]

metadata_table = [('name', 'VARCHAR(64) NOT NULL PRIMARY KEY'),
                  ('value', 'TEXT'),
                  ]

schema = {'table': table}


# ==============================================================================
#                              class LoopStore
# ==============================================================================

class LoopStore:
    """The packets, in a table of their own."""

    def __init__(self, database_dict, table_name='packets'):
        self.table_name = table_name
        try:
            self.connection = weedb.connect(database_dict)
        except weedb.NoDatabaseError:
            weedb.create(database_dict)
            self.connection = weedb.connect(database_dict)
        if self.table_name not in self.connection.tables():
            self._create()
        # Arrival order carries on from wherever the last run left it.
        self.seq = self._max_seq()

    @classmethod
    def open_with_config(cls, config_dict, binding=DEFAULT_BINDING,
                         archive_binding='wx_binding'):
        try:
            manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                      binding)
        except weewx.UnknownBinding:
            # Nothing configured, which is what an installation upgraded from an
            # earlier version looks like. Put the store beside the archive, in the same
            # kind of database, and say so.
            manager_dict = _beside_the_archive(config_dict, archive_binding)
            log.info("No '%s' in [DataBindings]. Keeping the packets in '%s'.",
                     binding, manager_dict['database_dict'].get('database_name'))
        return cls(manager_dict['database_dict'],
                   manager_dict.get('table_name', 'packets'))

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, etyp, einst, etb):
        self.close()

    # ------------------------------------------------------------------ writing

    def add(self, packet, source=None):
        """Store one LOOP packet. Returns its sequence number."""
        # 'dateTime' and 'usUnits' get columns of their own, because they are what the
        # store is queried by. The rest goes into the blob, whatever it is.
        data = {k: v for k, v in packet.items()
                if k not in ('dateTime', 'usUnits') and v is not None}
        self.seq += 1
        with weedb.Transaction(self.connection) as cursor:
            cursor.execute("INSERT INTO %s (seq, dateTime, usUnits, source, data) "
                           "VALUES (?, ?, ?, ?, ?)" % self.table_name,
                           (self.seq, int(packet['dateTime']), packet['usUnits'],
                            source, json.dumps(data)))
        return self.seq

    def trim(self, before_ts):
        """Drop everything older than a timestamp. Returns how many went."""
        with weedb.Transaction(self.connection) as cursor:
            cursor.execute("DELETE FROM %s WHERE dateTime < ?" % self.table_name,
                           (int(before_ts),))
            count = cursor.rowcount
        return count if count and count > 0 else 0

    # ------------------------------------------------------------------ reading

    def packets(self, start_ts, stop_ts):
        """Every packet in (start_ts, stop_ts], in the order it arrived."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT dateTime, usUnits, data FROM %s "
                           "WHERE dateTime > ? AND dateTime <= ? ORDER BY seq"
                           % self.table_name, (int(start_ts), int(stop_ts)))
            for row in cursor:
                packet = json.loads(row[2])
                packet['dateTime'] = row[0]
                packet['usUnits'] = row[1]
                yield packet
        finally:
            cursor.close()

    def span(self):
        """The oldest and newest timestamp held, or (None, None)."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT MIN(dateTime), MAX(dateTime) FROM %s"
                           % self.table_name)
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cursor.close()

    def count(self):
        """How many packets are held."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM %s" % self.table_name)
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    # ----------------------------------------------------------------- metadata

    def get_metadata(self, name, default=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT value FROM %s WHERE name = ?" % METADATA_TABLE,
                           (name,))
            row = cursor.fetchone()
            return row[0] if row else default
        finally:
            cursor.close()

    def set_metadata(self, name, value):
        with weedb.Transaction(self.connection) as cursor:
            cursor.execute("DELETE FROM %s WHERE name = ?" % METADATA_TABLE, (name,))
            cursor.execute("INSERT INTO %s (name, value) VALUES (?, ?)"
                           % METADATA_TABLE, (name, str(value)))

    # ------------------------------------------------------------------ private

    def _create(self):
        with weedb.Transaction(self.connection) as cursor:
            cursor.create_table(self.table_name, table)
            cursor.create_table(METADATA_TABLE, metadata_table)
        # Every query into the store is by time, so it needs the index. The primary
        # key is the sequence number, which is only good for arrival order.
        try:
            self.connection.execute("CREATE INDEX %s_dateTime ON %s(dateTime)"
                                    % (self.table_name, self.table_name))
        except weedb.DatabaseError as e:
            log.debug("No index on %s.dateTime: %s", self.table_name, e)
        log.info("Created table '%s' in database '%s'",
                 self.table_name, self.connection.database_name)

    def _max_seq(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT MAX(seq) FROM %s" % self.table_name)
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else 0
        finally:
            cursor.close()


def _beside_the_archive(config_dict, archive_binding='wx_binding'):
    """A manager dictionary for a store that sits next to the archive.

    Same kind of database, same place, a name of its own. Used when nothing has been
    configured, so that an upgraded installation needs no edit to weewx.conf.
    """
    manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                              archive_binding)
    database_dict = dict(manager_dict['database_dict'])
    name = database_dict.get('database_name', 'weewx.sdb')
    if name.endswith('.sdb'):
        database_dict['database_name'] = name[:-4] + '-loop.sdb'
    else:
        database_dict['database_name'] = name + '_loop'
    return {'database_dict': database_dict, 'table_name': 'packets'}


@contextlib.contextmanager
def _keeping_last_update(dbmanager):
    """Write an old record without winding the daily summaries back.

    Adding a record sets the 'lastUpdate' marker to its timestamp. For a record from
    hours or days ago that would leave weewx thinking the summaries are unfinished, and
    it would rebuild everything since. The summaries are put right here by hand, so the
    marker belongs where it was.
    """
    try:
        before = dbmanager._read_metadata('lastUpdate')
    except (AttributeError, weedb.DatabaseError):
        before = None
    try:
        yield
    finally:
        if before is not None:
            try:
                after = dbmanager._read_metadata('lastUpdate')
                if after is None or to_int(after) < to_int(before):
                    dbmanager._write_metadata('lastUpdate', before)
            except (AttributeError, weedb.DatabaseError, ValueError) as e:
                log.debug("Could not restore the daily summary marker: %s", e)


# ==============================================================================
#                            class StdLoopStore
# ==============================================================================

class StdLoopStore(weewx.engine.StdService):
    """Keep the LOOP packets, and let them fill in what the archive is missing."""

    def __init__(self, engine, config_dict):
        super().__init__(engine, config_dict)

        store_dict = config_dict.get('StdLoopStore', {})
        archive_dict = config_dict.get('StdArchive', {})

        self.data_binding = store_dict.get('data_binding', DEFAULT_BINDING)
        self.archive_binding = archive_dict.get('data_binding', 'wx_binding')
        self.retain_days = to_int(store_dict.get('retain_days', DEFAULT_RETAIN_DAYS))
        self.catchup_days = to_int(store_dict.get('catchup_days',
                                                  DEFAULT_CATCHUP_DAYS))
        # Which field of a packet says where it came from. Recorded for diagnosis;
        # nothing is decided by it.
        self.source_field = store_dict.get('source_field', 'station')

        self.archive_interval = to_int(archive_dict.get('archive_interval', 300))
        self.loop_hilo = to_bool(archive_dict.get('loop_hilo', True))

        self.store = None
        # Periods that received a packet this time round the loop.
        self.touched = set()

        if not to_bool(store_dict.get('enable', True)):
            log.info("Loop store is disabled.")
            return

        try:
            self.store = LoopStore.open_with_config(config_dict, self.data_binding,
                                                    self.archive_binding)
        except (weedb.DatabaseError, KeyError, AttributeError) as e:
            log.error("Cannot open the loop store using binding '%s': %s. Archive "
                      "records will not be checked against the packets.",
                      self.data_binding, e)
            return

        # When the packets were last trimmed. Kept in the store, so that restarting
        # often does not mean trimming often, and so that a long run still gets round
        # to it.
        self.last_trim = to_int(self.store.get_metadata(LAST_TRIM, 0)) or time.time()

        log.info("Loop store will use data binding %s, keeping %d days",
                 self.data_binding, self.retain_days)

        self.bind(weewx.STARTUP, self.startup)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.POST_LOOP, self.post_loop)

    def shutDown(self):
        if self.store:
            self.store.close()
            self.store = None

    # ------------------------------------------------------------------- events

    def startup(self, _event):
        """Look over what was stored while weewx was not running."""
        # The console's own archive interval wins, the same way StdArchive decides it.
        try:
            self.archive_interval = self.engine.console.archive_interval
        except (AttributeError, NotImplementedError):
            pass

        oldest, newest = self.store.span()
        if oldest is None:
            return
        horizon = max(oldest, time.time() - self.catchup_days * 86400)
        try:
            filled = self._reconcile(self._periods_between(horizon, newest))
        except weedb.DatabaseError as e:
            log.error("Could not check the archive against the stored packets: %s", e)
            return
        if filled:
            log.info("Filled in %d archive record(s) from stored packets.", filled)

    def new_loop_packet(self, event):
        """Store the packet, and note which period it belongs to."""
        packet = event.packet
        try:
            self.store.add(packet, source=packet.get(self.source_field))
        except (weedb.DatabaseError, TypeError, ValueError) as e:
            log.error("Cannot store a LOOP packet from %s: %s",
                      timestamp_to_string(packet.get('dateTime')), e)
            return
        self.touched.add(weeutil.weeutil.startOfInterval(packet['dateTime'],
                                                         self.archive_interval))

    def post_loop(self, _event):
        """StdArchive has had its turn. See whether the packets say anything more."""
        if not self.touched:
            return
        # A period that has not ended yet is still StdArchive's business.
        newest = self.store.span()[1] or 0
        due = {start for start in self.touched
               if start + self.archive_interval <= newest}
        self.touched -= due
        try:
            self._reconcile(sorted(due))
            self._trim()
        except weedb.DatabaseError as e:
            log.error("Could not check the archive against the stored packets: %s", e)

    # ------------------------------------------------------------------ the work

    def _reconcile(self, starts):
        """Make the archive say what the packets say, for these periods.

        An archive record is a function of the LOOP packets of its period, so every
        period with packets is worked out from them and compared with what is in the
        database. Where the two differ, the packets win: they are the measurements,
        the record is a summary of them.

        Returns how many records were written or put right.
        """
        if not starts:
            return 0
        dbmanager = self.engine.db_binder.get_manager(self.archive_binding)
        changed = []
        for start in sorted(starts):
            stop = start + self.archive_interval
            built = self._record_for(start, stop)
            if built is None:
                continue
            record, count = built
            existing = dbmanager.getRecord(stop)
            if existing is None:
                log.info("No archive record for %s. Writing one from %d stored "
                         "packet(s).", timestamp_to_string(stop), count)
            else:
                differing = self._differences(existing, record, dbmanager.sqlkeys)
                if not differing:
                    continue
                log.info("Archive record %s does not agree with its %d stored "
                         "packet(s) on %s. Working it out again.",
                         timestamp_to_string(stop), count, ', '.join(sorted(differing)))
            with _keeping_last_update(dbmanager):
                dbmanager.addRecord(record, update=True)
            changed.append(stop)

        if changed:
            self._rebuild_days(dbmanager, changed)
        return len(changed)

    def _rebuild_days(self, dbmanager, timestamps):
        """Build the daily summaries of the days touched, the way a live run builds
        them.

        A changed record cannot be worked into a summary that already counts the old
        one, so the day is built again from scratch. That is two steps, because a live
        run also takes two: the archive records make the sums and counts, and then the
        LOOP packets of each period lay their own highs and lows over the top, which
        are finer than any finished record can show. Doing only the first would quietly
        coarsen every high and low of the day.
        """
        days = sorted({weeutil.weeutil.startOfArchiveDay(ts) for ts in timestamps})
        for day_ts in days:
            day = datetime.date.fromtimestamp(day_ts)
            try:
                with _keeping_last_update(dbmanager):
                    dbmanager.backfill_day_summary(start_d=day, stop_d=day,
                                                   progress_fn=None)
                    self._restore_loop_extremes(dbmanager, day_ts)
            except (weewx.ViolatedPrecondition, weedb.DatabaseError,
                    AttributeError) as e:
                log.error("Could not rebuild the daily summary for %s: %s. Run "
                          "'weectl database rebuild-daily' for that day.", day, e)

    def _restore_loop_extremes(self, dbmanager, day_ts):
        """Lay the LOOP packets' highs and lows back over a rebuilt day."""
        stop_ts = day_ts + 86400
        oldest, newest = self.store.span()
        if oldest is None:
            return
        first = max(day_ts, weeutil.weeutil.startOfInterval(oldest,
                                                            self.archive_interval))
        last = min(stop_ts, newest)
        for start in self._periods_between(first, last):
            accumulator = self._accumulator_for(start, start + self.archive_interval)
            if accumulator is not None:
                with weedb.Transaction(dbmanager.connection) as cursor:
                    dbmanager._updateHiLo(accumulator, cursor)

    def _accumulator_for(self, start, stop):
        """The stored packets of a period, accumulated, or None if there are none."""
        accumulator = weewx.accum.Accum(weeutil.weeutil.TimeSpan(start, stop))
        count = 0
        for packet in self.store.packets(start, stop):
            try:
                accumulator.addRecord(packet, add_hilo=self.loop_hilo)
                count += 1
            except (weewx.accum.OutOfSpan, ValueError) as e:
                log.debug("Ignoring a stored packet from %s: %s",
                          timestamp_to_string(packet.get('dateTime')), e)
        if accumulator.isEmpty:
            return None
        accumulator.packet_count = count
        return accumulator

    def _record_for(self, start, stop):
        """What the stored packets for a period add up to.

        Returns (record, packet count), or None if there are no packets.
        """
        accumulator = self._accumulator_for(start, stop)
        if accumulator is None:
            return None
        record = accumulator.getRecord()
        record['interval'] = self.archive_interval / 60
        return record, accumulator.packet_count

    @staticmethod
    def _differences(existing, record, sqlkeys):
        """Where the archive record and the packets disagree.

        Only columns the database has, and only real differences: a float that comes
        back from SQLite a fraction out is the same number.
        """
        differing = set()
        for key, value in record.items():
            if key not in sqlkeys or key in ('dateTime', 'usUnits', 'interval'):
                continue
            was = existing.get(key)
            if was is None:
                if value is not None:
                    differing.add(key)
            elif value is None:
                continue
            elif isinstance(was, float) or isinstance(value, float):
                if abs(float(was) - float(value)) > 1e-9 * max(1.0, abs(float(was))):
                    differing.add(key)
            elif was != value:
                differing.add(key)
        return differing

    def _periods_between(self, start_ts, stop_ts):
        """Every archive period start in a span, oldest first."""
        starts = []
        start = weeutil.weeutil.startOfInterval(start_ts, self.archive_interval)
        while start + self.archive_interval <= stop_ts:
            starts.append(start)
            start += self.archive_interval
        return starts

    def _trim(self):
        """Drop packets older than the retention, once a day."""
        now = time.time()
        if now - self.last_trim < 86400:
            return
        self.last_trim = now
        self.store.set_metadata(LAST_TRIM, int(now))
        gone = self.store.trim(now - self.retain_days * 86400)
        if gone:
            log.info("Dropped %d stored packet(s) older than %d days.",
                     gone, self.retain_days)

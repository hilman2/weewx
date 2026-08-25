#
#    Copyright (c) 2026 Manuel Hilgert
#
#    See the file LICENSE.txt for your full rights.
#
"""The ingest table: every LOOP packet, kept so that records can be worked out.

WeeWX used to turn LOOP packets into an archive record in memory, one period at a
time, and then the packets were gone. The record was not a view of anything; it was
the only copy. A reading that arrived after its period had ended could not reach the
record it belonged to, and a restart in the middle of a period lost that period.

The packets now go into a table of their own, and `StdArchive` works every record out
from it. That makes the record what it always claimed to be, a summary of the
readings, and it means a late packet only has to reach the table: the record it
belongs to is written again from what is there.

The packets are held in two places, one after the other. While they are recent they
live in the table, indexed by time. When a day is older than the retention it is
written out as one gzipped NDJSON file, a thirtieth of the size, and dropped from the
table. Both are read, so it makes no difference which of the two a packet is in.

The table lives in a database of its own, so that its size and its retention have
nothing to do with the archive, and a backup can leave it out.
"""

import contextlib
import datetime
import glob
import gzip
import json
import logging
import os
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
# How long the packets stay in the database, where they can be queried by time.
DEFAULT_RETAIN_DAYS = 7
# How long they are kept afterwards, one gzipped NDJSON file per day. Measured on
# real LOOP packets, that is about a thirtieth of the space the database takes, so a
# year of eight-second data is a hundred megabytes or so.
DEFAULT_ARCHIVE_DAYS = 365
ARCHIVE_DIRNAME = 'packets'
METADATA_TABLE = 'loop_metadata'
# When the store was last trimmed, so that it survives a restart.
LAST_TRIM = 'lastTrim'
# The sequence number up to which the archive has been checked against the
# packets. Everything after it belongs to a period that may need working out
# again. Kept in the store, so an abrupt stop cannot lose the mark.
APPLIED_THROUGH = 'appliedThrough'

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

    def __init__(self, database_dict, table_name='packets', archive_dir=None):
        self.table_name = table_name
        # Where the days that have left the database are kept, one gzipped NDJSON
        # file each. Beside the database, because that is the directory weewx already
        # writes to and people already back up.
        self.archive_dir = archive_dir
        try:
            self.connection = weedb.connect(database_dict)
        except weedb.NoDatabaseError:
            weedb.create(database_dict)
            self.connection = weedb.connect(database_dict)
        self._make_it_quick()
        if self.table_name not in self.connection.tables():
            self._create()
        # Arrival order carries on from wherever the last run left it.
        self.seq = self._max_seq()

    @classmethod
    def open_with_config(cls, config_dict, binding=DEFAULT_BINDING,
                         archive_binding='wx_binding', archive_dir=None):
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
                   manager_dict.get('table_name', 'packets'),
                   archive_dir=archive_dir or _archive_dir_for(
                       manager_dict['database_dict']))

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
        """Move everything older than a timestamp out of the database.

        Each whole day goes into a gzipped NDJSON file of its own first, so that
        leaving the database is not the same as being thrown away. Returns how many
        packets left the database.
        """
        oldest = self.span()[0]
        if oldest is None:
            return 0
        count = 0
        day = weeutil.weeutil.startOfArchiveDay(oldest)
        while day + 86400 <= before_ts:
            # A day at a time, and only the packets that reached the file are dropped.
            # Deleting more than was written would lose them; deleting less would
            # leave them to be written a second time, and a packet counted twice
            # moves the average it belongs to.
            written = self.archive_day(day)
            if written or not self.archive_dir:
                count += self._drop_day(day)
            day += 86400
        return count

    def _drop_day(self, day_ts):
        """Delete a day's packets from the database. Returns how many went."""
        with weedb.Transaction(self.connection) as cursor:
            cursor.execute("DELETE FROM %s WHERE dateTime > ? AND dateTime <= ?"
                           % self.table_name, (int(day_ts) - 1, int(day_ts) + 86400))
            count = cursor.rowcount
        return count if count and count > 0 else 0

    def archive_day(self, day_ts):
        """Write a day's packets out as gzipped NDJSON. Returns how many were written.

        Appending is a second gzip member, which is what gzip concatenation is, and
        `zcat` reads it as one stream. That is how a packet which arrives after its
        day has been written still ends up in the right file.

        The packets stay in the database. Only `trim` takes them out, and only once
        this has said how many reached the file.
        """
        if not self.archive_dir:
            return 0
        packets = list(self.packets(day_ts - 1, day_ts + 86400, files=False))
        if not packets:
            return 0
        path = self.day_path(day_ts)
        try:
            if not os.path.isdir(self.archive_dir):
                os.makedirs(self.archive_dir)
            with gzip.open(path, 'ab') as fd:
                for packet in packets:
                    line = json.dumps(packet, separators=(',', ':')) + '\n'
                    fd.write(line.encode('utf-8'))
        except OSError as e:
            log.error("Cannot write %s: %s. Those packets will be lost when the "
                      "database is trimmed.", path, e)
            return 0
        log.info("Wrote %d packet(s) to %s", len(packets), path)
        return len(packets)

    def day_path(self, day_ts):
        """The file a day's packets live in."""
        name = time.strftime('%Y-%m-%d', time.localtime(day_ts)) + '.ndjson.gz'
        return os.path.join(self.archive_dir or '', name)

    def has_day(self, ts):
        """Whether the day a timestamp falls in has been written out."""
        if not self.archive_dir:
            return False
        return os.path.exists(self.day_path(weeutil.weeutil.startOfArchiveDay(ts)))

    def trim_archive(self, before_ts):
        """Delete day files older than a timestamp. Returns how many went."""
        if not self.archive_dir or not os.path.isdir(self.archive_dir):
            return 0
        gone = 0
        for path in glob.glob(os.path.join(self.archive_dir, '*.ndjson.gz')):
            try:
                day = time.mktime(time.strptime(
                    os.path.basename(path)[:10], '%Y-%m-%d'))
            except ValueError:
                continue
            if day + 86400 <= before_ts:
                try:
                    os.remove(path)
                    gone += 1
                except OSError as e:
                    log.warning("Cannot remove %s: %s", path, e)
        return gone

    # ------------------------------------------------------------------ reading

    def packets(self, start_ts, stop_ts, files=True):
        """Every packet in (start_ts, stop_ts], in the order it arrived.

        The written-out days come first, then whatever is still in the database. A
        packet is in one or the other, never both: a day is written out on its way out
        of the database.
        """
        if files:
            for packet in self._packets_from_files(start_ts, stop_ts):
                yield packet
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

    def _packets_from_files(self, start_ts, stop_ts):
        """The same, out of the day files a span touches."""
        if not self.archive_dir:
            return
        day = weeutil.weeutil.startOfArchiveDay(start_ts)
        while day <= stop_ts:
            path = self.day_path(day)
            day += 86400
            if not os.path.exists(path):
                continue
            try:
                with gzip.open(path, 'rt', encoding='utf-8') as fd:
                    for line in fd:
                        line = line.strip()
                        if not line:
                            continue
                        packet = json.loads(line)
                        if start_ts < packet.get('dateTime', 0) <= stop_ts:
                            yield packet
            except (OSError, ValueError) as e:
                log.error("Cannot read %s: %s", path, e)

    def times_after(self, seq):
        """The timestamps of every packet that arrived after a sequence number."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT DISTINCT dateTime FROM %s WHERE seq > ?"
                           % self.table_name, (int(seq),))
            return [row[0] for row in cursor]
        finally:
            cursor.close()

    def max_seq_through(self, stop_ts):
        """The highest sequence number among packets up to a time, or 0."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT MAX(seq) FROM %s WHERE dateTime <= ?"
                           % self.table_name, (int(stop_ts),))
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else 0
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

    def _make_it_quick(self):
        """Set SQLite up for a table that is written to constantly.

        A packet every couple of seconds means a commit every couple of seconds, and
        with the default settings each one waits for the disk. Measured on 2000
        packets: 10.8 ms each by default, 3.1 ms with a write-ahead log, and 0.03 ms
        with the log and synchronous=NORMAL. On an SD card the difference is larger
        still, and so is the wear.

        What NORMAL gives up is the last few seconds of packets if the power fails.
        A crash of weewx itself loses nothing, and the archive database is a separate
        file with its own settings, so none of this touches the records.
        """
        try:
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
        except weedb.DatabaseError as e:
            # MySQL and friends have neither, and do not need them.
            log.debug("Could not set the write-ahead log: %s", e)

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


def _archive_dir_for(database_dict):
    """Where a database's written-out days go: a directory beside it.

    Only SQLite says where it lives. For anything else there is no obvious place, so
    the days are not written out unless a directory is configured.
    """
    root = database_dict.get('SQLITE_ROOT')
    return os.path.join(root, ARCHIVE_DIRNAME) if root else None


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
def keeping_last_update(dbmanager):
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



def rebuild_day(dbmanager, ts, store, archive_interval, loop_hilo=True):
    """Build the daily summaries of a day again, the way a live run builds them.

    A record that has been written again cannot be worked into a summary that already
    counts the old one, so the day is built from scratch. That is two steps, because a
    live run also takes two: the archive records make the sums and counts, and then the
    LOOP packets of each period lay their own highs and lows over the top. Those are
    finer than any finished record can show, and doing only the first step would
    quietly coarsen every high and low of the day.
    """
    day_ts = weeutil.weeutil.startOfArchiveDay(ts)
    day = datetime.date.fromtimestamp(day_ts)
    try:
        with keeping_last_update(dbmanager):
            dbmanager.backfill_day_summary(start_d=day, stop_d=day, progress_fn=None)
            _restore_loop_extremes(dbmanager, day_ts, store, archive_interval,
                                   loop_hilo)
    except (weewx.ViolatedPrecondition, weedb.DatabaseError, AttributeError) as e:
        log.error("Could not rebuild the daily summary for %s: %s. Run "
                  "'weectl database rebuild-daily' for that day.", day, e)


def _restore_loop_extremes(dbmanager, day_ts, store, archive_interval, loop_hilo):
    """Lay the LOOP packets' highs and lows back over a rebuilt day."""
    oldest, newest = store.span()
    if oldest is None:
        return
    first = max(day_ts, weeutil.weeutil.startOfInterval(oldest, archive_interval))
    last = min(day_ts + 86400, newest)
    start = weeutil.weeutil.startOfInterval(first, archive_interval)
    while start + archive_interval <= last:
        stop = start + archive_interval
        accumulator = weewx.accum.Accum(weeutil.weeutil.TimeSpan(start, stop))
        for packet in store.packets(start, stop):
            try:
                accumulator.addRecord(packet, add_hilo=loop_hilo)
            except (weewx.accum.OutOfSpan, ValueError):
                pass
        if not accumulator.isEmpty:
            with weedb.Transaction(dbmanager.connection) as cursor:
                dbmanager._updateHiLo(accumulator, cursor)
        start += archive_interval

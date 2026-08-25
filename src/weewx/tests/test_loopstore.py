#
#    Copyright (c) 2026 Manuel Hilgert
#
#    See the file LICENSE.txt for your full rights.
#
"""Test the loop store against real databases.

Nothing here is mocked below the service: the archive and the store are real SQLite
databases, opened through the usual bindings, and the packets go through the same
sequence of events the engine dispatches. What is asserted is what a user would find
by opening the database afterwards.
"""

import datetime
import json
import os
import time

import configobj
import pytest

import weedb
import weeutil.weeutil
import weewx
import weewx.accum
import weewx.engine
import weewx.loopstore
import weewx.manager
from weeutil.weeutil import startOfInterval

os.environ['TZ'] = 'America/Los_Angeles'
time.tzset()

INTERVAL = 300
DELAY = 15
# An hour ago, on an archive boundary. Recent, because the store drops packets older
# than its retention, and a fixed date in the past would be trimmed away.
START = int(startOfInterval(time.time() - 3600, INTERVAL))


# ==============================================================================
#                              the harness
# ==============================================================================

class FakeConsole:
    """A console with no archive of its own, i.e. software record generation."""

    archive_interval = INTERVAL

    def genArchiveRecords(self, since_ts):
        raise NotImplementedError("No hardware archive")

    def genStartupRecords(self, since_ts):
        raise NotImplementedError("No hardware archive")


class LoggingConsole(FakeConsole):
    """A console that keeps its own archive, the way a Vantage does."""

    def __init__(self, records):
        self.records = list(records)
        self.catchups = 0

    def genArchiveRecords(self, since_ts):
        self.catchups += 1
        for record in self.records:
            if since_ts is None or record['dateTime'] > since_ts:
                yield dict(record)

    def genStartupRecords(self, since_ts):
        # Nothing to catch up on at startup: the logger has already been read. What
        # it produces from here on arrives through genArchiveRecords, alongside the
        # LOOP packets of the same period.
        return iter([])


class FakeEngine:
    """Enough engine to dispatch events, with a real database binder."""

    def __init__(self, config_dict, console=None):
        self.callbacks = {}
        self.console = console if console is not None else FakeConsole()
        self.db_binder = weewx.manager.DBBinder(config_dict)

    def bind(self, event_type, callback):
        self.callbacks.setdefault(event_type, []).append(callback)

    def dispatchEvent(self, event):
        for callback in self.callbacks.get(event.event_type, []):
            callback(event)

    def _get_console_time(self):
        return START


def make_config(tmp_path, **store_options):
    config = configobj.ConfigObj({
        'WEEWX_ROOT': str(tmp_path),
        'StdArchive': dict({
            'record_generation': 'software',
            'archive_interval': str(INTERVAL),
            'archive_delay': str(DELAY),
            'data_binding': 'wx_binding',
        }, **{k: str(v) for k, v in store_options.items()}),
        'Accumulator': {},
        'DatabaseTypes': {
            'SQLite': {'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)},
        },
        'Databases': {
            'archive_sqlite': {'database_name': 'test.sdb',
                               'database_type': 'SQLite'},
            'loop_sqlite': {'database_name': 'test-loop.sdb',
                            'database_type': 'SQLite'},
        },
        'DataBindings': {
            'wx_binding': {'database': 'archive_sqlite',
                           'table_name': 'archive',
                           'manager': 'weewx.manager.DaySummaryManager',
                           'schema': 'weewx.schemas.wview_extended.schema'},
            'loop_binding': {'database': 'loop_sqlite',
                             'table_name': 'packets',
                             'manager': 'weewx.loopstore.LoopStore',
                             'schema': 'weewx.loopstore.schema'},
        },
    })
    return config


def packet(timestamp, **values):
    p = {'dateTime': int(timestamp), 'usUnits': weewx.US, 'outTemp': 20.0}
    p.update(values)
    return p


class Station:
    """One StdArchive, fed the way StdEngine.run() feeds it."""

    def __init__(self, config_dict, console=None):
        self.config_dict = config_dict
        self.engine = FakeEngine(config_dict, console)
        self.archive = weewx.engine.StdArchive(self.engine, config_dict)
        self.engine.dispatchEvent(weewx.Event(weewx.STARTUP))
        self.engine.dispatchEvent(weewx.Event(weewx.PRE_LOOP))

    def feed(self, *packets):
        for p in packets:
            try:
                self.engine.dispatchEvent(weewx.Event(weewx.NEW_LOOP_PACKET, packet=p))
                self.engine.dispatchEvent(weewx.Event(weewx.CHECK_LOOP, packet=p))
            except weewx.engine.BreakLoop:
                self.engine.dispatchEvent(weewx.Event(weewx.POST_LOOP))
                self.engine.dispatchEvent(weewx.Event(weewx.PRE_LOOP))
        return self

    def close(self):
        self.archive.shutDown()
        self.engine.db_binder.close()

    # ------------------------------------------------------------ what is stored

    @property
    def manager(self):
        return self.engine.db_binder.get_manager('wx_binding')

    def records(self):
        return list(self.manager.genBatchRecords())

    def record(self, ts):
        return self.manager.getRecord(ts)

    def times(self):
        return [r['dateTime'] for r in self.records()]

    def day_summary(self, obs, ts):
        """The daily summary row for one observation type, as a dict."""
        day = weeutil.weeutil.startOfArchiveDay(ts)
        row = self.manager.getSql("SELECT min, max, sum, count, wsum, sumtime "
                                  "FROM archive_day_%s WHERE dateTime = ?" % obs,
                                  (day,))
        if row is None:
            return None
        return dict(zip(('min', 'max', 'sum', 'count', 'wsum', 'sumtime'), row))


@pytest.fixture
def config(tmp_path):
    return make_config(tmp_path)


@pytest.fixture
def station(config):
    s = Station(config)
    yield s
    s.close()


# ==============================================================================
#                            the store on its own
# ==============================================================================

def test_a_packet_comes_back_as_it_went_in(tmp_path):
    store = weewx.loopstore.LoopStore(
        {'database_name': 'plain.sdb', 'database_type': 'SQLite',
         'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)})
    store.add(packet(START + 10, outTemp=12.5, extraTemp3=3.0), source='garden')
    out = list(store.packets(START, START + INTERVAL))
    store.close()

    assert len(out) == 1
    assert out[0]['dateTime'] == START + 10
    assert out[0]['outTemp'] == 12.5
    assert out[0]['extraTemp3'] == 3.0
    assert out[0]['usUnits'] == weewx.US


def test_packets_come_back_in_the_order_they_arrived(tmp_path):
    """Two packets with the same timestamp still have an order, for 'last'."""
    store = weewx.loopstore.LoopStore(
        {'database_name': 'order.sdb', 'database_type': 'SQLite',
         'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)})
    store.add(packet(START + 10, outTemp=1.0))
    store.add(packet(START + 10, outTemp=2.0))
    store.add(packet(START + 5, outTemp=3.0))       # late, but arrived third
    out = [p['outTemp'] for p in store.packets(START, START + INTERVAL)]
    store.close()

    assert out == [1.0, 2.0, 3.0]


def test_the_sequence_carries_on_after_a_restart(tmp_path):
    args = ({'database_name': 'seq.sdb', 'database_type': 'SQLite',
             'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)},)
    first = weewx.loopstore.LoopStore(*args)
    first.add(packet(START))
    first.add(packet(START + 1))
    first.close()

    second = weewx.loopstore.LoopStore(*args)
    seq = second.add(packet(START + 2))
    second.close()

    assert seq == 3


def test_old_packets_are_dropped(tmp_path):
    store = weewx.loopstore.LoopStore(
        {'database_name': 'trim.sdb', 'database_type': 'SQLite',
         'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)})
    for n in range(10):
        store.add(packet(START + n * 86400))
    gone = store.trim(START + 5 * 86400)
    remaining = store.count()
    store.close()

    assert gone == 5
    assert remaining == 5


def test_the_store_makes_its_own_database(tmp_path):
    path = str(tmp_path / 'brand-new.sdb')
    assert not os.path.exists(path)
    store = weewx.loopstore.LoopStore(
        {'database_name': 'brand-new.sdb', 'database_type': 'SQLite',
         'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)})
    store.close()

    assert os.path.exists(path)


def test_it_falls_back_to_a_database_beside_the_archive(config):
    """An installation upgraded from an earlier version has no loop_binding."""
    del config['DataBindings']['loop_binding']
    store = weewx.loopstore.LoopStore.open_with_config(config)
    name = store.connection.database_name
    store.close()

    assert name == 'test-loop.sdb'


# ==============================================================================
#                          nothing changes for a normal run
# ==============================================================================

def test_a_normal_run_writes_one_record_per_period(station):
    """Every packet on time, which is what most stations do all day."""
    station.feed(*[packet(START + n * 20, outTemp=20.0 + n % 7) for n in range(1, 90)])

    assert station.times() == [START + INTERVAL, START + 2 * INTERVAL,
                               START + 3 * INTERVAL, START + 4 * INTERVAL,
                               START + 5 * INTERVAL]


def test_the_store_holds_every_packet(station):
    packets = [packet(START + n * 20) for n in range(1, 40)]
    station.feed(*packets)

    assert station.archive.store.count() == len(packets)


# ==============================================================================
#                                filling in
# ==============================================================================

def test_a_late_reading_reaches_the_record_it_belongs_to(station):
    """The case this is all for: a second console, a minute behind."""
    # The fast console fills the first period.
    station.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 10)])
    # Into the next period, so the first one gets written.
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    assert station.record(START + INTERVAL)['extraTemp3'] is None

    # Now the slow console reports, for a period that is already in the database.
    station.feed(packet(START + 100, extraTemp3=41.2))
    station.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))

    assert station.record(START + INTERVAL)['extraTemp3'] == 41.2


def test_a_late_reading_is_worked_into_the_average(station):
    """A record is what its packets say, so a late one changes the average."""
    station.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 10)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    assert station.record(START + INTERVAL)['outTemp'] == 10.0

    station.feed(packet(START + 100, outTemp=20.0))
    station.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))

    # Nine readings at 10 and one at 20.
    assert station.record(START + INTERVAL)['outTemp'] == pytest.approx(11.0)


def test_late_rain_is_not_lost(station):
    """The reason a record has to be worked out again rather than filled in.

    Rain is a sum. A late tip of the bucket cannot be added to a record that already
    holds a rain value, so under a fill-only rule those millimetres are gone for good.
    """
    station.feed(*[packet(START + n * 20, rain=0.1) for n in range(1, 10)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    assert station.record(START + INTERVAL)['rain'] == pytest.approx(0.9)

    station.feed(packet(START + 100, rain=0.5))
    station.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))

    assert station.record(START + INTERVAL)['rain'] == pytest.approx(1.4)
    assert station.day_summary('rain', START)['sum'] == pytest.approx(1.4)


def test_a_period_with_no_record_at_all_gets_one(station):
    """What a restart in the middle of an archive period leaves behind."""
    station.feed(*[packet(START + n * 20, outTemp=11.0) for n in range(1, 10)])
    # Delete the record behind the service's back, as losing it would.
    with weedb.Transaction(station.manager.connection) as cursor:
        cursor.execute("DELETE FROM archive")
    station.manager._sync()
    assert station.record(START + INTERVAL) is None

    # A packet for that period turns up, and the gap is noticed.
    station.feed(packet(START + 200, outTemp=11.0))
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))

    written = station.record(START + INTERVAL)
    assert written is not None
    assert written['outTemp'] == 11.0


def test_what_was_lost_while_weewx_was_down_is_found_at_startup(config):
    """The packets outlive the process, so the period is rebuilt on the way up."""
    first = Station(config)
    first.feed(*[packet(START + n * 20, outTemp=15.0) for n in range(1, 10)])
    # Stopped before the period ended: StdArchive never wrote a record.
    assert first.record(START + INTERVAL) is None
    first.close()

    # A packet from the next period, as the console would have sent while weewx was
    # down. It is what makes the earlier period finished.
    store = weewx.loopstore.LoopStore.open_with_config(config)
    store.add(packet(START + INTERVAL + 30))
    store.close()

    second = Station(config)
    second.archive.post_loop(None)
    found = second.record(START + INTERVAL)
    second.close()

    assert found is not None
    assert found['outTemp'] == 15.0


# ==============================================================================
#                       the daily summaries stay correct
# ==============================================================================

def test_filling_in_does_not_count_the_record_twice(station):
    """The trap: adding to a record adds its contribution to the day a second time."""
    station.feed(*[packet(START + n * 20, outTemp=10.0, rain=1.0)
                   for n in range(1, 10)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    before = station.day_summary('rain', START + INTERVAL)
    assert before['sum'] == 9.0

    # Fill in a field of that record. The rain in it must not be counted again.
    station.feed(packet(START + 100, extraTemp3=41.2))
    station.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))

    assert station.day_summary('rain', START + INTERVAL)['sum'] == before['sum']


def test_the_daily_summary_gains_the_field_that_was_filled_in(station):
    station.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 10)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    assert station.day_summary('extraTemp3', START + INTERVAL)['count'] == 0

    station.feed(packet(START + 100, extraTemp3=41.2))
    station.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))

    day = station.day_summary('extraTemp3', START + INTERVAL)
    assert day['count'] == 1
    assert day['min'] == 41.2
    assert day['max'] == 41.2


def test_the_daily_summary_marker_is_not_wound_back(station):
    """Writing an old record must not make weewx think the summaries are unfinished."""
    station.feed(*[packet(START + n * 20) for n in range(1, 10)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    station.feed(*[packet(START + INTERVAL + n * 20) for n in range(2, 10)])
    station.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))
    marker = int(station.manager._read_metadata('lastUpdate'))

    # Now fill in a field of the first record, which is two periods old.
    station.feed(packet(START + 100, extraTemp3=41.2))
    station.feed(packet(START + 3 * INTERVAL + 10),
                 packet(START + 3 * INTERVAL + 200))

    assert int(station.manager._read_metadata('lastUpdate')) >= marker


OBSERVED = ('outTemp', 'rain', 'extraTemp3', 'windSpeed')


def test_the_daily_summary_agrees_with_a_full_rebuild(station):
    """The real check.

    Whatever the store did, the summaries must be what the archive records add up to.
    Anything else is drift, and drift is what nobody notices.
    """
    station.feed(*[packet(START + n * 20, outTemp=10.0 + n % 5, rain=0.1,
                          windSpeed=float(n % 9))
                   for n in range(1, 15)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    # Two late readings for a period already written, then a couple of periods more.
    station.feed(packet(START + 100, extraTemp3=41.2),
                 packet(START + 150, extraTemp3=42.8))
    station.feed(*[packet(START + 2 * INTERVAL + n * 20, rain=0.1)
                   for n in range(1, 15)])
    station.feed(packet(START + 3 * INTERVAL + 10),
                 packet(START + 3 * INTERVAL + 200))

    kept = {obs: station.day_summary(obs, START) for obs in OBSERVED}
    # The two late readings did arrive, averaged into the one record they belong to.
    assert station.record(START + INTERVAL)['extraTemp3'] == pytest.approx(42.0)
    assert kept['extraTemp3']['count'] == 1

    # Now throw the summaries away and build them again from the archive alone.
    station.manager.drop_daily()
    station.engine.db_binder.close()
    rebuilt = weewx.manager.open_manager_with_config(station.config_dict,
                                                     'wx_binding', initialize=True)
    rebuilt.backfill_day_summary(progress_fn=None)
    day = weeutil.weeutil.startOfArchiveDay(START)
    rows = {}
    for obs in OBSERVED:
        row = rebuilt.getSql("SELECT min, max, sum, count, wsum, sumtime "
                             "FROM archive_day_%s WHERE dateTime = ?" % obs, (day,))
        rows[obs] = dict(zip(('min', 'max', 'sum', 'count', 'wsum', 'sumtime'), row))
    rebuilt.close()

    # Only the additive columns. 'min' and 'max' are deliberately finer than a rebuild
    # can reconstruct: with loop_hilo set, they come from the LOOP packets, while a
    # rebuild only ever sees finished archive records. That gap is there without the
    # store too, and is not this test's business.
    for obs in OBSERVED:
        for column in ('sum', 'count', 'wsum', 'sumtime'):
            assert kept[obs][column] == pytest.approx(rows[obs][column]),                 '%s.%s' % (obs, column)


def test_the_result_is_what_arriving_on_time_would_have_given(tmp_path):
    """The promise, stated as a test.

    Two stations get the same readings. For one they all arrive in order; for the
    other, three of them turn up after their archive period has been written. When the
    store has done its work the two databases must agree, record for record and
    summary for summary. Anything else is the late data leaving a mark, and it must
    not leave one.
    """
    on_time = []
    for n in range(1, 15):
        on_time.append(packet(START + n * 20, outTemp=10.0 + n % 5, rain=0.1,
                              windSpeed=float(n % 9), extraTemp3=30.0 + n))
    tail = [packet(START + INTERVAL + 10), packet(START + INTERVAL + 200),
            packet(START + 2 * INTERVAL + 10), packet(START + 2 * INTERVAL + 200)]

    punctual = Station(make_config(tmp_path / 'punctual'))
    punctual.feed(*(on_time + tail))
    expected_records = {r['dateTime']: dict(r) for r in punctual.records()}
    expected_days = {obs: punctual.day_summary(obs, START) for obs in OBSERVED}
    punctual.close()

    # The same readings, but three of them held back until after their period closed.
    early, late = on_time[:-3], on_time[-3:]
    delayed = Station(make_config(tmp_path / 'delayed'))
    delayed.feed(*early)
    delayed.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    delayed.feed(*late)
    delayed.feed(packet(START + 2 * INTERVAL + 10),
                 packet(START + 2 * INTERVAL + 200))
    actual_records = {r['dateTime']: dict(r) for r in delayed.records()}
    actual_days = {obs: delayed.day_summary(obs, START) for obs in OBSERVED}
    delayed.close()

    assert sorted(actual_records) == sorted(expected_records)
    for ts in expected_records:
        for key, value in expected_records[ts].items():
            if isinstance(value, float):
                assert actual_records[ts][key] == pytest.approx(value), '%s %s' % (ts, key)
            else:
                assert actual_records[ts][key] == value, '%s %s' % (ts, key)
    for obs in OBSERVED:
        assert actual_days[obs] == pytest.approx(expected_days[obs]), obs


def test_the_daily_summary_keeps_the_loop_highs_after_a_rebuild(station):
    """Rebuilding a day must not coarsen its highs and lows.

    With loop_hilo set, the extremes come from the LOOP packets, and are finer than
    anything a finished archive record can show. Rebuilding from the records alone
    would quietly lose that, for the whole day.
    """
    station.feed(*[packet(START + n * 20, outTemp=10.0 + n % 5)
                   for n in range(1, 15)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    before = station.day_summary('outTemp', START)

    # Force a rebuild with a late reading that carries rain. Its temperature is inside
    # the range already seen, so the extremes should not move. Reconciled by hand, so
    # that no further record is written and the two measurements are of the same thing.
    station.feed(packet(START + 100, outTemp=12.0, rain=0.5))
    station.archive.post_loop(None)
    after = station.day_summary('outTemp', START)

    # The extremes came from LOOP packets and did not move.
    assert after['min'] == before['min']
    assert after['max'] == before['max']

    # And they really are finer than the records: every archive average of the day sits
    # strictly inside them, so a rebuild from records alone would have narrowed the day.
    averages = [r['outTemp'] for r in station.records() if r['outTemp'] is not None]
    assert min(averages) > after['min']
    assert max(averages) <= after['max']


def test_a_period_older_than_the_retention_is_left_alone(station, caplog):
    """The one that would destroy data.

    A packet from two months ago arrives. Its period is long past, and the store no
    longer holds the rest of it. Working the record out again would replace a hundred
    readings with the one that turned up late.
    """
    import logging

    station.feed(*[packet(START + n * 20, outTemp=10.0, rain=0.1)
                   for n in range(1, 15)])
    station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    intact = dict(station.record(START + INTERVAL))

    # Two months back, well outside the seven days of retention.
    long_ago = START - 60 * 86400
    with weedb.Transaction(station.manager.connection) as cursor:
        cursor.execute("INSERT INTO archive (dateTime, usUnits, interval, outTemp, "
                       "rain) VALUES (?, ?, ?, ?, ?)",
                       (long_ago + INTERVAL, weewx.US, INTERVAL / 60, 5.0, 3.0))
    station.manager._sync()

    with caplog.at_level(logging.WARNING):
        station.archive.store.add(packet(long_ago + 100, outTemp=99.0, rain=0.0))
        station.archive.latest_ts = None
        station.archive._pick_up_where_we_left_off()
        station.archive.post_loop(None)

    old_record = station.record(long_ago + INTERVAL)
    assert old_record['outTemp'] == 5.0        # untouched
    assert old_record['rain'] == 3.0           # the month's rain is still there
    assert 'archive_days' in caplog.text
    assert 'weectl import' in caplog.text
    # And the recent record is unaffected by all of this.
    assert station.record(START + INTERVAL)['outTemp'] == intact['outTemp']


def test_raising_the_retention_lets_older_periods_through(tmp_path):
    """Somebody whose source lags by weeks sets the retention to cover it."""
    config = make_config(tmp_path, retain_days=120)
    station = Station(config)
    try:
        long_ago = START - 60 * 86400
        station.archive.store.add(packet(long_ago + 100, outTemp=7.0))
        station.archive.store.add(packet(long_ago + 200, outTemp=9.0))
        station.archive.store.add(packet(START + 10))
        station.archive._pick_up_where_we_left_off()
        station.archive.post_loop(None)

        written = station.record(long_ago + INTERVAL)
        assert written is not None
        assert written['outTemp'] == pytest.approx(8.0)
    finally:
        station.close()


def test_the_mark_survives_a_restart(config):
    """What has been dealt with is not gone over again after a restart."""
    first = Station(config)
    first.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 10)])
    first.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    mark = first.archive.store.get_metadata(weewx.loopstore.APPLIED_THROUGH)
    first.close()

    assert mark is not None and int(mark) > 0

    second = Station(config)
    try:
        # Nothing new has arrived, so the startup pass has nothing to do.
        assert len(second.archive._finished_periods()) == 0
    finally:
        second.close()


def test_a_packet_that_arrives_while_stopped_is_dealt_with_on_the_way_up(config):
    """No mark for it, so the next start finds it."""
    first = Station(config)
    first.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 10)])
    first.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
    assert first.record(START + INTERVAL)['extraTemp3'] is None
    # Straight into the store, as a driver would have done just before the stop.
    first.archive.store.add(packet(START + 100, extraTemp3=41.2))
    first.close()

    second = Station(config)
    second.archive.post_loop(None)
    found = second.record(START + INTERVAL)['extraTemp3']
    second.close()

    assert found == 41.2


# ==============================================================================
#                       the days that leave the database
# ==============================================================================

def a_store(tmp_path, name='days.sdb', **kw):
    return weewx.loopstore.LoopStore(
        {'database_name': name, 'database_type': 'SQLite',
         'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)},
        archive_dir=str(tmp_path / 'packets'), **kw)


def test_trimming_writes_the_day_out_before_dropping_it(tmp_path):
    """Leaving the database must not be the same as being thrown away."""
    store = a_store(tmp_path)
    long_ago = START - 40 * 86400
    for n in range(1, 6):
        store.add(packet(long_ago + n * 20, outTemp=float(n)))
    store.add(packet(START))

    moved = store.trim(START - 86400)
    day_file = store.day_path(weeutil.weeutil.startOfArchiveDay(long_ago))
    kept = list(store.packets(long_ago, long_ago + INTERVAL))
    store.close()

    assert moved == 5
    assert os.path.exists(day_file)
    assert [p['outTemp'] for p in kept] == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_the_day_file_is_plain_gzipped_ndjson(tmp_path):
    """One JSON object per line, readable with zcat and anything else."""
    import gzip

    store = a_store(tmp_path)
    long_ago = START - 40 * 86400
    store.add(packet(long_ago + 10, outTemp=12.5))
    store.add(packet(long_ago + 20, outTemp=13.5))
    store.trim(START - 86400)
    path = store.day_path(weeutil.weeutil.startOfArchiveDay(long_ago))
    store.close()

    with gzip.open(path, 'rt', encoding='utf-8') as fd:
        lines = [line for line in fd if line.strip()]
    assert len(lines) == 2
    assert [json.loads(line)['outTemp'] for line in lines] == [12.5, 13.5]


def test_a_packet_that_arrives_after_its_day_was_written_joins_it(tmp_path):
    """Appending is a second gzip member, and reads back as one stream."""
    store = a_store(tmp_path)
    long_ago = START - 40 * 86400
    store.add(packet(long_ago + 10, outTemp=1.0))
    store.trim(START - 86400)

    # Late, for a day that has already gone to file.
    store.add(packet(long_ago + 20, outTemp=2.0))
    store.trim(START - 86400)
    back = [p['outTemp'] for p in store.packets(long_ago, long_ago + INTERVAL)]
    store.close()

    assert back == [1.0, 2.0]


def test_a_day_that_could_not_be_written_stays_in_the_database(tmp_path):
    """Losing the file must not also mean losing the packets."""
    store = a_store(tmp_path)
    long_ago = START - 40 * 86400
    store.add(packet(long_ago + 10, outTemp=1.0))
    store.add(packet(START))
    # A directory that cannot be created, so the write fails.
    store.archive_dir = '/nowhere/at/all/packets'

    moved = store.trim(START - 86400)
    left = store.count()
    store.close()

    assert moved == 0
    assert left == 2          # both still there, nothing thrown away


def test_a_packet_is_never_in_both_places(tmp_path):
    """A packet counted twice would move the average it belongs to."""
    store = a_store(tmp_path)
    long_ago = START - 40 * 86400
    for n in range(1, 6):
        store.add(packet(long_ago + n * 20, outTemp=float(n)))
    store.add(packet(START))

    store.trim(START - 86400)
    # Trimming again must not write the same packets out a second time.
    store.trim(START - 86400)
    back = [p['outTemp'] for p in store.packets(long_ago, long_ago + INTERVAL)]
    store.close()

    assert back == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_old_day_files_are_deleted(tmp_path):
    store = a_store(tmp_path)
    for days in (400, 300, 40):
        when = START - days * 86400
        store.add(packet(when + 10))
        store.trim(when + 86400)
    before = len(os.listdir(str(tmp_path / 'packets')))

    gone = store.trim_archive(START - 365 * 86400)
    after = len(os.listdir(str(tmp_path / 'packets')))
    store.close()

    assert before == 3
    assert gone == 1
    assert after == 2


def test_a_record_is_still_put_right_from_a_day_file(config, tmp_path):
    """The point of keeping the days: a late packet from weeks back still works."""
    station = Station(config)
    try:
        store = station.archive.store
        weeks_ago = START - 30 * 86400
        for n in range(1, 10):
            store.add(packet(weeks_ago + n * 20, outTemp=10.0))
        # Push that day out of the database and into a file.
        store.trim(START - 7 * 86400)
        assert store.has_day(weeks_ago)

        # A late reading for it, and a recent packet so the period counts as closed.
        store.add(packet(weeks_ago + 100, extraTemp3=41.2))
        store.add(packet(START + 10))
        station.archive._pick_up_where_we_left_off()
        station.archive.post_loop(None)

        written = station.record(weeks_ago + INTERVAL)
        assert written is not None
        assert written['extraTemp3'] == 41.2
        # Nine readings of 10 out of the file, plus the late one at 20. Had the file
        # not been read, the record would say 20.
        assert written['outTemp'] == pytest.approx(11.0)
    finally:
        station.close()


def test_zero_means_the_day_files_are_kept_for_ever(tmp_path):
    config = make_config(tmp_path, archive_days=0)
    station = Station(config)
    try:
        store = station.archive.store
        long_ago = START - 400 * 86400
        store.add(packet(long_ago + 10, outTemp=3.0))
        store.trim(long_ago + 2 * 86400)
        assert store.has_day(long_ago)

        # A trim that would delete anything older than "now" leaves it alone.
        station.archive.last_trim = 0
        station.archive._trim()

        assert store.has_day(long_ago)
        assert [p['outTemp'] for p in store.packets(long_ago, long_ago + INTERVAL)]             == [3.0]
    finally:
        station.close()


def test_without_a_directory_nothing_is_written_out(tmp_path):
    """A store with nowhere to put the days just drops them, as before."""
    store = weewx.loopstore.LoopStore(
        {'database_name': 'nodir.sdb', 'database_type': 'SQLite',
         'driver': 'weedb.sqlite', 'SQLITE_ROOT': str(tmp_path)},
        archive_dir=None)
    long_ago = START - 40 * 86400
    store.add(packet(long_ago + 10))
    moved = store.trim(START - 86400)
    left = store.count()
    store.close()

    assert moved == 1
    assert left == 0


# ==============================================================================
#                            switches and failures
# ==============================================================================

def test_an_ingest_table_that_cannot_be_opened_says_so(tmp_path):
    """Every record is worked out from it, so there is nothing to carry on with."""
    config = make_config(tmp_path)
    config['Databases']['loop_sqlite']['database_name'] = '/nowhere/at/all/x.sdb'

    with pytest.raises(weewx.engine.InitializationError) as caught:
        Station(config)

    assert 'ingest table' in str(caught.value)


# ==============================================================================
#                      hardware that keeps its own archive
# ==============================================================================

def hardware_record(ts, **values):
    r = {'dateTime': int(ts), 'usUnits': weewx.US, 'interval': INTERVAL / 60,
         'outTemp': 55.5}
    r.update(values)
    return r


def test_a_logger_record_is_used_as_it_stands(tmp_path):
    """Where the console keeps its own archive, its record is the record.

    The packets are still kept, but they do not overrule what the logger says. It
    measured over the whole period; the packets are only what reached us.
    """
    console = LoggingConsole([hardware_record(START + INTERVAL, outTemp=41.0)])
    config = make_config(tmp_path, record_generation='hardware')
    station = Station(config, console=console)
    try:
        station.feed(*[packet(START + n * 20, outTemp=99.0) for n in range(1, 10)])
        station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))

        written = station.record(START + INTERVAL)
        assert written['outTemp'] == 41.0        # the logger's figure, not 99.0
        assert console.catchups >= 1
    finally:
        station.close()


def test_a_logger_record_is_augmented_from_the_packets(tmp_path):
    """The logger has fewer fields than the packets do. The rest comes from them."""
    console = LoggingConsole([hardware_record(START + INTERVAL)])
    config = make_config(tmp_path, record_generation='hardware')
    station = Station(config, console=console)
    try:
        station.feed(*[packet(START + n * 20, extraTemp3=12.5) for n in range(1, 10)])
        station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))

        written = station.record(START + INTERVAL)
        assert written['outTemp'] == 55.5        # from the logger
        assert written['extraTemp3'] == 12.5     # from the packets
    finally:
        station.close()


def test_a_console_with_no_archive_falls_back_to_the_packets(tmp_path):
    """NotImplementedError means work it out from the packets after all."""
    config = make_config(tmp_path, record_generation='hardware')
    station = Station(config)
    try:
        station.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 10)])
        station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))

        assert station.record(START + INTERVAL)['outTemp'] == 10.0
    finally:
        station.close()


def test_a_logger_record_is_not_overwritten_by_a_late_packet(tmp_path):
    """The logger is the authority for its own period, late packets or not."""
    console = LoggingConsole([hardware_record(START + INTERVAL, outTemp=41.0)])
    config = make_config(tmp_path, record_generation='hardware')
    station = Station(config, console=console)
    try:
        station.feed(*[packet(START + n * 20) for n in range(1, 10)])
        station.feed(packet(START + INTERVAL + 10), packet(START + INTERVAL + 200))
        assert station.record(START + INTERVAL)['outTemp'] == 41.0

        station.feed(packet(START + 100, outTemp=-40.0))
        station.feed(packet(START + 2 * INTERVAL + 10),
                     packet(START + 2 * INTERVAL + 200))

        assert station.record(START + INTERVAL)['outTemp'] == 41.0
    finally:
        station.close()

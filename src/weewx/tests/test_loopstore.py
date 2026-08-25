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


class FakeEngine:
    """Enough engine to dispatch events, with a real database binder."""

    def __init__(self, config_dict):
        self.callbacks = {}
        self.console = FakeConsole()
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
        'StdArchive': {
            'record_generation': 'software',
            'archive_interval': str(INTERVAL),
            'archive_delay': str(DELAY),
            'data_binding': 'wx_binding',
        },
        'StdLoopStore': {k: str(v) for k, v in store_options.items()},
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
    """StdArchive and StdLoopStore, fed the way StdEngine.run() feeds them."""

    def __init__(self, config_dict, with_store=True):
        self.config_dict = config_dict
        self.engine = FakeEngine(config_dict)
        self.archive = weewx.engine.StdArchive(self.engine, config_dict)
        self.store_service = weewx.loopstore.StdLoopStore(self.engine, config_dict) \
            if with_store else None
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
        if self.store_service:
            self.store_service.shutDown()
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

def test_a_normal_run_writes_the_same_records(config, tmp_path):
    """With every packet on time, the store changes nothing at all."""
    packets = [packet(START + n * 20, outTemp=20.0 + n % 7) for n in range(1, 90)]

    without = Station(make_config(tmp_path / 'a'), with_store=False).feed(*packets)
    plain = {r['dateTime']: dict(r) for r in without.records()}
    without.close()

    with_store = Station(make_config(tmp_path / 'b')).feed(*packets)
    stored = {r['dateTime']: dict(r) for r in with_store.records()}
    with_store.close()

    assert sorted(plain) == sorted(stored)
    for ts in plain:
        assert plain[ts] == stored[ts], ts


def test_the_store_holds_every_packet(station):
    packets = [packet(START + n * 20) for n in range(1, 40)]
    station.feed(*packets)

    assert station.store_service.store.count() == len(packets)


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

    second = Station(config)
    # The startup pass needs to see the period as finished, so give it a later packet
    # first. In a real restart the console supplies that within seconds.
    second.store_service.store.add(packet(START + INTERVAL + 30))
    second.store_service.startup(None)
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
    station.store_service.post_loop(None)
    after = station.day_summary('outTemp', START)

    # The extremes came from LOOP packets and did not move.
    assert after['min'] == before['min']
    assert after['max'] == before['max']

    # And they really are finer than the records: every archive average of the day sits
    # strictly inside them, so a rebuild from records alone would have narrowed the day.
    averages = [r['outTemp'] for r in station.records() if r['outTemp'] is not None]
    assert min(averages) > after['min']
    assert max(averages) <= after['max']


# ==============================================================================
#                            switches and failures
# ==============================================================================

def test_it_can_be_turned_off(tmp_path):
    config = make_config(tmp_path, enable='false')
    station = Station(config)
    station.feed(*[packet(START + n * 20) for n in range(1, 20)])
    store_missing = station.store_service.store is None
    station.close()

    assert store_missing
    assert not os.path.exists(str(tmp_path / 'test-loop.sdb'))


def test_a_store_that_cannot_be_opened_does_not_stop_weewx(tmp_path, caplog):
    import logging

    config = make_config(tmp_path)
    config['Databases']['loop_sqlite']['database_name'] = '/nowhere/at/all/x.sdb'
    with caplog.at_level(logging.ERROR):
        station = Station(config)
        station.feed(*[packet(START + n * 20, outTemp=10.0) for n in range(1, 20)])
        station.feed(packet(START + INTERVAL + 200))
        written = station.record(START + INTERVAL)
        station.close()

    assert written['outTemp'] == 10.0        # the archive is unaffected
    assert 'Cannot open the loop store' in caplog.text

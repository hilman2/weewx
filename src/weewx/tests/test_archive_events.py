#
#    See the file LICENSE.txt for your full rights.
#
"""Exercise archive events with a second database that stores fewer observations."""

import copy
import socket

import configobj
import pytest

import weewx.accum
import weewx.engine


BASE_TIME = 1700000100


@pytest.fixture
def archive_engine(tmp_path, monkeypatch, request):
    schema = {'dateTime': 'INTEGER NOT NULL PRIMARY KEY',
              'usUnits': 'INTEGER NOT NULL', 'interval': 'INTEGER NOT NULL',
              'outTemp': 'REAL'}
    primary_schema = dict(schema, inTemp='REAL')
    config = configobj.ConfigObj({
        'Station': {'altitude': [0, 'meter'], 'latitude': 0, 'longitude': 0},
        'Engine': {'Services': {group: [] for group in weewx.all_service_groups}},
        'StdConvert': {'target_unit': 'METRICWX'},
        'StdArchive': {'record_generation': 'software', 'archive_interval': 300,
                       'archive_delay': 15, 'no_catchup': True, 'log_success': False},
        'DataBindings': {
            'wx_binding': {'database': 'primary', 'schema': primary_schema},
            'alt_binding': {'database': 'secondary', 'schema': schema}},
        'Databases': {
            name: {'driver': 'weedb.sqlite', 'database_name': str(tmp_path / (name + '.sdb'))}
            for name in ['primary', 'secondary']}})
    config.merge(getattr(request, 'param', {}))
    monkeypatch.setattr(weewx.accum, 'accum_dict', copy.deepcopy(weewx.accum.accum_dict))
    previous_timeout = socket.getdefaulttimeout()
    engine = weewx.engine.DummyEngine(config)
    try:
        engine.service_obj.append(weewx.engine.StdConvert(engine, config))
        engine.service_obj.append(weewx.engine.StdCalibrate(engine, config))
        events = []
        # Read the attachment before StdArchive handles the event.
        engine.bind(weewx.NEW_ARCHIVE_RECORD,
                    lambda event: events.append((event.record['dateTime'],
                                                  getattr(event, 'accumulator', None))))
        archive = weewx.engine.StdArchive(engine, config)
        engine.service_obj.append(archive)
        monkeypatch.setattr(engine.console, 'getTime', lambda: BASE_TIME + 310, raising=False)
        engine.db_binder.get_manager('wx_binding', initialize=True)
        engine.db_binder.get_manager('alt_binding', initialize=True)
        yield engine, archive, events
    finally:
        engine.shutDown()
        socket.setdefaulttimeout(previous_timeout)


def feed_loop_interval(engine, unit_system=weewx.METRICWX):
    engine.dispatchEvent(weewx.Event(weewx.PRE_LOOP))
    for offset, temperature in [(310, 0), (590, 20), (620, 100)]:
        packet = {'dateTime': BASE_TIME + offset, 'usUnits': unit_system,
                  'outTemp': temperature * 1.8 + 32 if unit_system == weewx.US else temperature,
                  'inTemp': 69.8 if unit_system == weewx.US else 21}
        engine.dispatchEvent(weewx.Event(weewx.NEW_LOOP_PACKET, packet=packet))
        if offset < 600:
            engine.dispatchEvent(weewx.Event(weewx.CHECK_LOOP, packet=packet))
        else:
            with pytest.raises(weewx.engine.BreakLoop):
                engine.dispatchEvent(weewx.Event(weewx.CHECK_LOOP, packet=packet))


def hardware_record(timestamp):
    return {'dateTime': timestamp, 'usUnits': weewx.US, 'interval': 5,
            'outTemp': 50, 'inTemp': 69.8}


@pytest.mark.parametrize('generation', ['software', 'hardware', 'fallback'])
@pytest.mark.parametrize('unit_system', [weewx.METRICWX, weewx.US])
@pytest.mark.parametrize('augmentation', [True, False])
def test_secondary_database_keeps_loop_extremes(archive_engine, monkeypatch,
                                               generation, unit_system, augmentation):
    engine, archive, events = archive_engine
    primary = engine.db_binder.get_manager('wx_binding')
    secondary = engine.db_binder.get_manager('alt_binding')
    archive.record_generation = 'software' if generation == 'software' else 'hardware'
    archive.record_augmentation = augmentation

    def generate_records(_since):
        if generation == 'fallback':
            raise NotImplementedError
        yield hardware_record(BASE_TIME + 600)

    monkeypatch.setattr(engine.console, 'genArchiveRecords', generate_records, raising=False)

    def save_secondary(event):
        record = {key: event.record[key] for key in secondary.sqlkeys}
        secondary.addRecord(record, accumulator=getattr(event, 'accumulator', None),
                            log_success=False)

    engine.bind(weewx.NEW_ARCHIVE_RECORD, save_secondary)
    feed_loop_interval(engine, unit_system)
    accumulator = archive.old_accumulator
    original_stats = accumulator['outTemp'].getStatsTuple()
    engine.dispatchEvent(weewx.Event(weewx.POST_LOOP))

    # The archived average is 10 C, so it cannot supply the LOOP extremes 0 and 20.
    for manager in [secondary, primary]:
        minimum, min_timestamp, maximum, max_timestamp, total, count, weighted_total, weight \
            = manager.getSql('SELECT min, mintime, max, maxtime, sum, count, wsum, sumtime '
                              'FROM archive_day_outTemp')
        assert (minimum, maximum, total, weighted_total) == pytest.approx((0, 20, 10, 3000))
        assert (min_timestamp, max_timestamp, count, weight) \
            == (BASE_TIME + 310, BASE_TIME + 590, 1, 300)
        assert manager.getRecord(BASE_TIME + 600)['outTemp'] == pytest.approx(10)
    assert primary.getRecord(BASE_TIME + 600)['inTemp'] == pytest.approx(21)
    assert 'inTemp' not in secondary.sqlkeys
    assert events == [(BASE_TIME + 600, accumulator)]
    assert accumulator.unit_system == weewx.METRICWX
    assert accumulator['outTemp'].getStatsTuple() == original_stats
    assert archive.old_accumulator is None


def test_hardware_catchup_only_attaches_matching_accumulator(archive_engine):
    engine, archive, events = archive_engine
    feed_loop_interval(engine)
    accumulator = archive.old_accumulator
    timestamps = [BASE_TIME + offset for offset in [300, 599, 600, 900]]
    archive._catchup(lambda since: (hardware_record(timestamp) for timestamp in timestamps))
    assert events == list(zip(timestamps, [None, None, accumulator, None]))


def test_startup_records_have_no_accumulator(archive_engine, monkeypatch):
    engine, archive, events = archive_engine
    archive.no_catchup = False
    startup_events = []
    engine.bind(weewx.NEW_ARCHIVE_RECORD, startup_events.append)
    monkeypatch.setattr(engine.console, 'genStartupRecords',
                        lambda since: iter([hardware_record(BASE_TIME + 300)]), raising=False)
    engine.dispatchEvent(weewx.Event(weewx.STARTUP))
    assert events == [(BASE_TIME + 300, None)]
    assert startup_events[0].accumulator is None


def test_loop_hilo_disabled_keeps_statistics_unavailable(archive_engine):
    engine, archive, events = archive_engine
    archive.loop_hilo = False
    feed_loop_interval(engine)
    engine.dispatchEvent(weewx.Event(weewx.POST_LOOP))
    accumulator = events[0][1]
    assert accumulator['outTemp'].min is None
    assert accumulator['outTemp'].max is None
    assert accumulator['outTemp'].avg == pytest.approx(10)


@pytest.mark.parametrize('offset', [300, 600])
@pytest.mark.parametrize('augmentation', [True, False])
def test_archive_event_with_no_accumulator_has_no_loop_statistics(archive_engine,
                                                                 offset, augmentation):
    engine, archive, _events = archive_engine
    archive.record_augmentation = augmentation
    feed_loop_interval(engine)
    timestamp = BASE_TIME + offset
    record = hardware_record(timestamp)
    del record['inTemp']
    event = weewx.Event(weewx.NEW_ARCHIVE_RECORD, record=record, origin='hardware',
                       accumulator=None)

    engine.dispatchEvent(event)

    primary = engine.db_binder.get_manager('wx_binding')
    saved_record = primary.getRecord(timestamp)
    assert primary.getSql('SELECT min, max FROM archive_day_outTemp') \
        == pytest.approx((10, 10))
    assert saved_record['outTemp'] == pytest.approx(10)
    assert saved_record['inTemp'] is None


def test_hardware_archive_preserves_console_average(archive_engine, monkeypatch):
    engine, archive, _events = archive_engine
    archive.record_generation = 'hardware'
    record = hardware_record(BASE_TIME + 600)
    record['outTemp'] = 59
    monkeypatch.setattr(engine.console, 'genArchiveRecords',
                        lambda since: iter([record]), raising=False)
    feed_loop_interval(engine)
    accumulator = archive.old_accumulator

    engine.dispatchEvent(weewx.Event(weewx.POST_LOOP))

    primary = engine.db_binder.get_manager('wx_binding')
    assert primary.getRecord(BASE_TIME + 600)['outTemp'] == pytest.approx(15)
    assert accumulator.getRecord()['outTemp'] == pytest.approx(10)
    assert primary.getSql('SELECT min, max FROM archive_day_outTemp') \
        == pytest.approx((0, 20))


@pytest.mark.parametrize('archive_engine', [
    {'StdCalibrate': {'Corrections': {'outTemp': ['outTemp + 5', 'archive']}}}
], indirect=True)
@pytest.mark.parametrize('generation', ['software', 'hardware'])
def test_archive_only_calibration_preserves_loop_statistics(archive_engine, monkeypatch,
                                                           generation):
    engine, archive, _events = archive_engine
    archive.record_generation = generation
    monkeypatch.setattr(engine.console, 'genArchiveRecords',
                        lambda since: iter([hardware_record(BASE_TIME + 600)]), raising=False)
    feed_loop_interval(engine)
    accumulator = archive.old_accumulator
    original_stats = accumulator['outTemp'].getStatsTuple()

    engine.dispatchEvent(weewx.Event(weewx.POST_LOOP))

    primary = engine.db_binder.get_manager('wx_binding')
    saved_record = primary.getRecord(BASE_TIME + 600)
    assert saved_record['dateTime'] == accumulator.timespan.stop
    assert saved_record['outTemp'] == pytest.approx(15)
    assert accumulator.getRecord()['outTemp'] == pytest.approx(10)
    assert accumulator['outTemp'].getStatsTuple() == original_stats
    assert primary.getSql('SELECT min, max FROM archive_day_outTemp') \
        == pytest.approx((0, 20))


def test_database_batch_ignores_unrelated_loop_statistics(archive_engine):
    engine, archive, _events = archive_engine
    feed_loop_interval(engine)
    secondary = engine.db_binder.get_manager('alt_binding')
    records = [{'dateTime': timestamp, 'usUnits': weewx.METRICWX,
                'interval': 5, 'outTemp': 10}
               for timestamp in [BASE_TIME - 86400 + 300, BASE_TIME + 300]]

    secondary.addRecord(records, accumulator=archive.old_accumulator, log_success=False)

    assert list(secondary.genSql('SELECT min, max FROM archive_day_outTemp ORDER BY dateTime')) \
        == [(10, 10), (10, 10)]

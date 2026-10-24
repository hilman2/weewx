#
#    See the file LICENSE.txt for your full rights.
#
"""Test tag notation for season iteration."""

import calendar
import time
import types

import Cheetah.Template
import pytest

import weewx.tags


@pytest.fixture
def time_binder(monkeypatch):
    with monkeypatch.context() as context:
        context.setenv('TZ', 'UTC')
        time.tzset()
        manager = types.SimpleNamespace(
            firstGoodStamp=lambda: calendar.timegm((2024, 1, 1, 0, 0, 0)))
        yield weewx.tags.TimeBinder(
            lambda data_binding: manager,
            calendar.timegm((2024, 12, 31, 0, 0, 0)), lat=51.0, lon=13.0)
    time.tzset()


def render_seasons(time_binder, expression):
    source = ('#for $season in ' + expression + '\n'
              '$season.start.format("%Y-%m-%d")/$season.end.format("%Y-%m-%d")\n'
              '#end for\n')
    template = Cheetah.Template.Template(source, searchList=[time_binder])
    return str(template).splitlines()


@pytest.mark.parametrize('argument', [
    '', '()', '(season_type=None)', '(season_type="m")',
    '(season_type="Meteorological")',
])
def test_alltime_meteorological_seasons(time_binder, argument):
    assert render_seasons(time_binder, '$alltime.seasons' + argument) == [
        '2024-01-01/2024-03-01',
        '2024-03-01/2024-06-01',
        '2024-06-01/2024-09-01',
        '2024-09-01/2024-12-01',
        '2024-12-01/2025-03-01',
    ]


@pytest.mark.parametrize('argument', [
    '("a")', '(season_type="a")', '(season_type="Astronomical")',
])
def test_alltime_astronomical_seasons(time_binder, argument):
    assert render_seasons(time_binder, '$alltime.seasons' + argument) == [
        '2024-01-01/2024-03-20',
        '2024-03-20/2024-06-20',
        '2024-06-20/2024-09-22',
        '2024-09-22/2024-12-21',
        '2024-12-21/2025-03-20',
    ]


@pytest.mark.parametrize('argument', ['', '()', '(season_type=None)'])
@pytest.mark.parametrize('season_type, expected', [
    ('m', [
        '2024-03-01/2024-06-01',
        '2024-06-01/2024-09-01',
        '2024-09-01/2024-12-01',
        '2024-12-01/2025-03-01',
    ]),
    ('a', [
        '2024-03-20/2024-06-20',
        '2024-06-20/2024-09-22',
        '2024-09-22/2024-12-21',
        '2024-12-21/2025-03-20',
    ]),
])
def test_seasonsyear_inherits_season_type(time_binder, argument, season_type, expected):
    expression = '$seasonsyear(season_type="%s").seasons%s' % (season_type, argument)
    assert render_seasons(time_binder, expression) == expected


@pytest.mark.parametrize('parent_type, iterator_type, expected', [
    ('a', 'm', [
        '2024-03-20/2024-06-01',
        '2024-06-01/2024-09-01',
        '2024-09-01/2024-12-01',
        '2024-12-01/2025-03-01',
        '2025-03-01/2025-06-01',
    ]),
    ('m', 'a', [
        '2024-03-01/2024-03-20',
        '2024-03-20/2024-06-20',
        '2024-06-20/2024-09-22',
        '2024-09-22/2024-12-21',
        '2024-12-21/2025-03-20',
    ]),
])
def test_seasons_overrides_parent_type(time_binder, parent_type, iterator_type, expected):
    expression = ('$seasonsyear(season_type="%s").seasons(season_type="%s")'
                  % (parent_type, iterator_type))
    assert render_seasons(time_binder, expression) == expected

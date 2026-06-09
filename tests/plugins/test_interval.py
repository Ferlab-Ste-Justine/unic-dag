"""Unit tests for IntervalTimetable."""
# pylint: disable=missing-function-docstring,protected-access,invalid-name
from __future__ import annotations

from datetime import timedelta

import pendulum
import pytest
from freezegun import freeze_time

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DataInterval, TimeRestriction

from timetables.interval import IntervalTimetable

TZ = pendulum.timezone("America/Montreal")
START = pendulum.datetime(2026, 3, 13, 3, 0, 0, tz=TZ)
INTERVAL = timedelta(weeks=4)


def _tt() -> IntervalTimetable:
    return IntervalTimetable(interval=INTERVAL)


def _restriction(earliest=START, latest=None, catchup=True) -> TimeRestriction:
    return TimeRestriction(earliest=earliest, latest=latest, catchup=catchup)


# --- construction --------------------------------------------------------
def test_construction_stores_interval():
    tt = _tt()
    assert tt._interval == INTERVAL


@pytest.mark.parametrize("bad_interval", [timedelta(0), timedelta(seconds=-1)])
def test_construction_rejects_non_positive_interval(bad_interval):
    with pytest.raises(AirflowTimetableInvalid):
        IntervalTimetable(interval=bad_interval)


def test_construction_rejects_non_timedelta_interval():
    with pytest.raises(AirflowTimetableInvalid):
        IntervalTimetable(interval=42)  # type: ignore[arg-type]


# --- alignment helpers (anchor passed explicitly) -----------------------
@pytest.mark.parametrize(
    "instant,expected",
    [
        (START, START),
        (START - timedelta(days=1), START),
        (START + INTERVAL - timedelta(seconds=1), START),
        (START + INTERVAL, START + INTERVAL),
        (START + 2 * INTERVAL - timedelta(seconds=1), START + INTERVAL),
        (START + 5 * INTERVAL, START + 5 * INTERVAL),
    ],
)
def test_align_to_prev(instant, expected):
    assert _tt()._align_to_prev(instant, anchor=START) == expected


@pytest.mark.parametrize(
    "instant,expected",
    [
        (START, START),
        (START - timedelta(days=1), START),
        (START + timedelta(seconds=1), START + INTERVAL),
        (START + INTERVAL, START + INTERVAL),
        (START + INTERVAL + timedelta(seconds=1), START + 2 * INTERVAL),
        (START + 5 * INTERVAL, START + 5 * INTERVAL),
    ],
)
def test_align_to_next(instant, expected):
    assert _tt()._align_to_next(instant, anchor=START) == expected


# --- infer_manual_data_interval -----------------------------------------
def test_infer_manual_data_interval_uses_delta_semantic():
    """Manual triggers have no anchor info, so the interval ends at run_after."""
    run_after = START + INTERVAL + timedelta(days=3)
    di = _tt().infer_manual_data_interval(run_after=run_after)
    assert di == DataInterval(start=run_after - INTERVAL, end=run_after)


# --- next_dagrun_info: chaining off last_automated_data_interval --------
def test_next_dagrun_info_chains_from_aligned_last_end():
    last = DataInterval(start=START, end=START + INTERVAL)
    info = _tt().next_dagrun_info(
        last_automated_data_interval=last,
        restriction=_restriction(),
    )
    assert info is not None
    assert info.data_interval == DataInterval(
        start=START + INTERVAL, end=START + 2 * INTERVAL
    )


def test_next_dagrun_info_realigns_after_anchor_shift():
    """When the DAG's start_date is shifted, next slot re-anchors to the new grid.

    Simulate a DAG that previously ran at 03:00 (last end = 2026-04-10 03:00),
    then the user shifts the DAG's start_date to 04:00. The next run's data
    interval end must align to the NEW grid (04:00), not chain off the old
    03:00 end.
    """
    old_last_end = START + INTERVAL  # 2026-04-10 03:00
    shifted_anchor = START.add(hours=1)  # 2026-03-13 04:00
    info = _tt().next_dagrun_info(
        last_automated_data_interval=DataInterval(start=START, end=old_last_end),
        restriction=_restriction(earliest=shifted_anchor),
    )
    assert info is not None
    # Grid is [shifted_anchor, shifted_anchor + INTERVAL, ...].
    # _align_to_prev(2026-04-10 03:00 on the 04:00-anchored grid) lands on
    # shifted_anchor (03:00 of Apr 10 is before 04:00 of Apr 10, so the
    # previous aligned slot is shifted_anchor = 2026-03-13 04:00).
    assert info.data_interval.end.hour == 4
    assert info.data_interval == DataInterval(
        start=shifted_anchor, end=shifted_anchor + INTERVAL
    )


# --- next_dagrun_info: daylight-saving-time stability -------------------
# A 03:00 schedule must keep firing at 03:00 *local* across DST transitions
# (like a cron schedule), not drift by an hour the way absolute-duration
# (timedelta) arithmetic does. 2026 transitions in America/Montreal:
# spring-forward Mar 8, fall-back Nov 1.
DST_SUMMER_ANCHOR = pendulum.datetime(2026, 9, 4, 3, 0, 0, tz=TZ)  # 03:00 EDT
DST_WINTER_ANCHOR = pendulum.datetime(2026, 2, 13, 3, 0, 0, tz=TZ)  # 03:00 EST


def test_next_dagrun_info_holds_wall_clock_across_fall_back():
    """An interval whose end crosses fall-back (Nov 1) lands at 03:00, not 02:00."""
    last_end = pendulum.datetime(2026, 10, 30, 3, 0, 0, tz=TZ)  # slot before fall-back
    info = _tt().next_dagrun_info(
        last_automated_data_interval=DataInterval(start=last_end - INTERVAL, end=last_end),
        restriction=_restriction(earliest=DST_SUMMER_ANCHOR),
    )
    assert info is not None
    assert info.data_interval.end.hour == 3
    assert info.data_interval.end == pendulum.datetime(2026, 11, 27, 3, 0, 0, tz=TZ)


def test_next_dagrun_info_holds_wall_clock_after_fall_back():
    """Both endpoints of an interval entirely after fall-back stay at 03:00 local."""
    last_end = pendulum.datetime(2026, 11, 27, 3, 0, 0, tz=TZ)  # 03:00 EST
    info = _tt().next_dagrun_info(
        last_automated_data_interval=DataInterval(start=last_end - INTERVAL, end=last_end),
        restriction=_restriction(earliest=DST_SUMMER_ANCHOR),
    )
    assert info is not None
    assert info.data_interval.start.hour == 3
    assert info.data_interval.start == last_end
    assert info.data_interval.end.hour == 3
    assert info.data_interval.end == pendulum.datetime(2026, 12, 25, 3, 0, 0, tz=TZ)


def test_next_dagrun_info_holds_wall_clock_across_spring_forward():
    """An interval whose end crosses spring-forward (Mar 8) lands at 03:00, not 04:00."""
    last_end = DST_WINTER_ANCHOR  # 2026-02-13 03:00 EST, the anchor slot
    info = _tt().next_dagrun_info(
        last_automated_data_interval=DataInterval(start=last_end - INTERVAL, end=last_end),
        restriction=_restriction(earliest=DST_WINTER_ANCHOR),
    )
    assert info is not None
    assert info.data_interval.end.hour == 3
    assert info.data_interval.end == pendulum.datetime(2026, 3, 13, 3, 0, 0, tz=TZ)


# --- next_dagrun_info: catchup=True, first run --------------------------
def test_next_dagrun_info_first_run_catchup_true_starts_at_anchor():
    info = _tt().next_dagrun_info(
        last_automated_data_interval=None,
        restriction=_restriction(earliest=START, catchup=True),
    )
    assert info is not None
    assert info.data_interval == DataInterval(start=START, end=START + INTERVAL)


# --- next_dagrun_info: catchup=False ------------------------------------
def test_next_dagrun_info_first_run_catchup_false_skips_to_latest_completed():
    """catchup=False with no past run and anchor in the past should fire the
    most recently completed interval (whose end has already passed)."""
    # Freeze "now" two days past anchor + 3*INTERVAL (3 intervals completed).
    frozen_now = START + 3 * INTERVAL + timedelta(days=2)
    with freeze_time(frozen_now):
        info = _tt().next_dagrun_info(
            last_automated_data_interval=None,
            restriction=_restriction(earliest=START, catchup=False),
        )
    assert info is not None
    # Latest interval whose end <= now is [START + 2*INTERVAL, START + 3*INTERVAL].
    assert info.data_interval == DataInterval(
        start=START + 2 * INTERVAL, end=START + 3 * INTERVAL
    )


def test_next_dagrun_info_first_run_catchup_false_first_interval_in_progress():
    """If now is before anchor + interval, the first interval hasn't ended;
    schedule it at the anchor so run_after = anchor + interval (future)."""
    frozen_now = START + timedelta(days=3)
    with freeze_time(frozen_now):
        info = _tt().next_dagrun_info(
            last_automated_data_interval=None,
            restriction=_restriction(earliest=START, catchup=False),
        )
    assert info is not None
    assert info.data_interval == DataInterval(start=START, end=START + INTERVAL)


# --- next_dagrun_info: edge cases ---------------------------------------
def test_next_dagrun_info_returns_none_when_earliest_is_none():
    info = _tt().next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )
    assert info is None


def test_next_dagrun_info_returns_none_when_past_latest():
    info = _tt().next_dagrun_info(
        last_automated_data_interval=None,
        restriction=_restriction(
            earliest=START, latest=START - timedelta(days=1), catchup=True
        ),
    )
    assert info is None


# --- serialize / deserialize --------------------------------------------
def test_serialize_round_trip():
    original = _tt()
    rebuilt = IntervalTimetable.deserialize(original.serialize())
    assert rebuilt == original
    assert rebuilt._interval == INTERVAL


def test_serialize_only_carries_interval():
    assert _tt().serialize() == {"interval": INTERVAL.total_seconds()}


def test_summary_includes_interval():
    assert "28 days" in _tt().summary


def test_summary_drops_zero_time_of_day():
    # str(timedelta(weeks=4)) is "28 days, 0:00:00"; the "0:00:00" is misleading
    # (it is not the run time) and must not leak into the UI label.
    assert "0:00:00" not in _tt().summary


def test_description_has_no_schedule_prefix():
    # Airflow prepends "Schedule: " to the description in the UI tooltip, so the
    # description itself must not start with it (else it doubles up).
    assert not _tt().description.startswith("Schedule:")


@pytest.mark.parametrize(
    "interval, expected",
    [
        # whole-day intervals (the common case) drop the time-of-day noise
        (timedelta(weeks=4), "28 days"),
        (timedelta(weeks=2, days=3), "17 days"),
        (timedelta(days=1), "1 day"),
        # single non-day units
        (timedelta(hours=12), "12 hours"),
        (timedelta(minutes=30), "30 minutes"),
        (timedelta(seconds=1), "1 second"),
        # mixed units, with zero components skipped
        (timedelta(days=3, hours=2), "3 days, 2 hours"),
        (timedelta(hours=2, seconds=5), "2 hours, 5 seconds"),
        (timedelta(minutes=90), "1 hour, 30 minutes"),
        # every unit present, all singular
        (timedelta(days=1, hours=1, minutes=1, seconds=1), "1 day, 1 hour, 1 minute, 1 second"),
        # every unit present, all plural
        (timedelta(days=10, hours=5, minutes=30, seconds=15), "10 days, 5 hours, 30 minutes, 15 seconds"),
    ],
)
def test_humanize_formats_components(interval, expected):
    assert IntervalTimetable._humanize(interval) == expected

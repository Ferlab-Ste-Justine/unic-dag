"""Custom Airflow timetable that anchors a fixed-interval schedule on the DAG's ``start_date``.

Background
----------
Airflow's stock ``DeltaDataIntervalTimetable`` (used when ``schedule_interval``
is a ``datetime.timedelta``) computes the next run by chaining off the
previous run's data-interval end: ``next.start = last.end``. Once a DAG has
any history, the schedule is anchored on the most recent actual run rather
than on the DAG's ``start_date``. As a result, editing ``start_date`` to
shift the run time of a DAG already in production does NOT move the schedule
- the only workaround has been clearing DAG history in the Airflow UI.

``IntervalTimetable`` recomputes every run from the DAG's ``start_date`` (read
from ``TimeRestriction.earliest`` on each scheduler tick) on the grid
``start_date + k * interval``. Editing ``start_date`` in code is enough to
re-align the next run on the next scheduler tick - no history clearing
required.

DST behaviour
-------------
The grid is computed in naive (DST-free) wall-clock space and then localized to
the ``start_date`` timezone, so the *wall-clock* run time is held constant across
daylight-saving transitions: a schedule anchored at 03:00 keeps firing at 03:00
local year-round, exactly like a cron schedule. This differs from a plain
``timedelta`` schedule (``DeltaDataIntervalTimetable``), which adds an absolute
duration and therefore drifts the run by an hour after each DST transition.

Usage
-----
Requires ``start_date=`` set on the DAG itself (not just on tasks). ::

    from datetime import timedelta
    import pendulum
    from timetables import IntervalTimetable

    dag = DAG(
        dag_id="curated_cscmed",
        schedule=IntervalTimetable(interval=timedelta(weeks=4)),
        start_date=pendulum.datetime(2026, 3, 13, 3, tz="America/Montreal"),
        catchup=False,
        ...
    )

To shift the run time later, edit only ``start_date=`` on the DAG. The
timetable re-anchors on the next scheduler tick.
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import Any

from pendulum import DateTime

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class IntervalTimetable(Timetable):
    """Schedule on the grid ``DAG.start_date + k * interval`` for non-negative ``k``.

    Example grid with ``start_date = 2026-03-13 03:00`` and ``interval = 4 weeks``:

    ===  =================
    k    slot
    ===  =================
    0    2026-03-13 03:00
    1    2026-04-10 03:00
    2    2026-05-08 03:00
    3    2026-06-05 03:00
    ===  =================

    Alignment turns an arbitrary instant into a slot index ``k`` by computing
    ``elapsed / interval`` and rounding -- ``floor`` for the slot at-or-before
    the instant, ``ceil`` for the slot at-or-after.
    """

    description: str = "Every fixed interval, anchored on the DAG's start_date."

    def __init__(self, interval: timedelta) -> None:
        if not isinstance(interval, timedelta) or interval <= timedelta(0):
            raise AirflowTimetableInvalid(f"interval must be a positive timedelta, got {interval!r}")
        self._interval = interval
        self.description = f"Every {self._humanize(interval)}, anchored on the DAG's start_date."

    @staticmethod
    def _humanize(interval: timedelta) -> str:
        """Render ``interval`` as a compact human string, e.g. ``28 days`` or ``3 days, 2 hours``.

        Built from the non-zero day/hour/minute/second components so the
        misleading ``0:00:00`` time-of-day from ``str(timedelta)`` is dropped --
        that suffix reads like a run time but isn't (the run time is set by the
        DAG's ``start_date``, not the interval).
        """
        total = int(interval.total_seconds())
        days, rem = divmod(total, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, seconds = divmod(rem, 60)
        units = [(days, "day"), (hours, "hour"), (minutes, "minute"), (seconds, "second")]
        parts = [f"{value} {name}{'s' if value != 1 else ''}" for value, name in units if value]
        return ", ".join(parts) if parts else "0 seconds"

    @property
    def summary(self) -> str:
        return f"every {self._humanize(self._interval)}"

    def __eq__(self, other: Any) -> bool:
        """
        Return if the intervals match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, IntervalTimetable):
            return NotImplemented
        return self._interval == other._interval

    def serialize(self) -> dict[str, Any]:
        return {"interval": self._interval.total_seconds()}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> IntervalTimetable:
        return cls(interval=timedelta(seconds=data["interval"]))

    @staticmethod
    def _localize(naive: DateTime, timezone) -> DateTime:
        """Attach ``timezone`` to a naive wall-clock ``DateTime``, picking the correct UTC offset."""
        return timezone.convert(naive)

    def _grid_slot(self, anchor: DateTime, k: int) -> DateTime:
        """The ``k``-th schedule slot: ``anchor``'s wall clock advanced ``k`` intervals.

        Arithmetic is done in naive (DST-free) wall-clock space and then localized,
        so the *wall-clock* time is held constant across DST transitions -- a 03:00
        schedule keeps firing at 03:00, like cron -- rather than holding the absolute
        elapsed duration (which would drift the run by an hour after each transition).
        """
        return self._localize(anchor.naive() + k * self._interval, anchor.timezone)

    def _slot_at(self, instant: DateTime, anchor: DateTime, round_fn) -> DateTime:
        """Return grid slot ``k`` where ``k = round_fn(wall_clock_elapsed / interval)``.

        Pass ``math.floor`` to land on the slot at-or-before ``instant``, or
        ``math.ceil`` to land on the slot at-or-after. Clamped to ``anchor``
        when ``instant`` is at or before the anchor (no earlier slot exists).

        ``elapsed`` is measured in naive wall-clock space (``instant`` rendered in
        the anchor's timezone, tz stripped) so the division lands on an exact
        integer ``k`` at grid points regardless of any DST transition in between.
        """
        if instant <= anchor:
            return anchor
        timezone = anchor.timezone
        elapsed = (instant.in_timezone(timezone).naive() - anchor.naive()).total_seconds()
        period = self._interval.total_seconds()
        k = round_fn(elapsed / period)
        return self._grid_slot(anchor, k)

    def _align_to_prev(self, instant: DateTime, anchor: DateTime) -> DateTime:
        """Largest (``floor``) ``anchor + k*interval`` slot ``<= instant``.

        Example with ``anchor = 2026-03-13 03:00`` and ``interval = 4 weeks``:
        an instant of ``2026-04-25 12:00`` is ``~1.55`` intervals past the
        anchor, so ``floor`` picks ``k=1`` → ``2026-04-10 03:00``.
        """
        return self._slot_at(instant, anchor, math.floor)

    def _align_to_next(self, instant: DateTime, anchor: DateTime) -> DateTime:
        """Smallest (``ceil``) ``anchor + k*interval`` slot ``>= instant``.

        Same example as ``_align_to_prev``: an instant of ``2026-04-25 12:00``
        is ``~1.55`` intervals past the anchor, so ``ceil`` picks ``k=2`` →
        ``2026-05-08 03:00``.
        """
        return self._slot_at(instant, anchor, math.ceil)

    def _skip_to_latest(self, anchor: DateTime) -> DateTime:
        """For ``catchup=False``: start of the most recently completed interval.

        Mirrors ``CronDataIntervalTimetable._skip_to_latest`` semantics: return
        the start of the latest interval whose ``end`` is ``<= now`` so the
        corresponding DagRun fires immediately. If the first interval is still
        in progress, return ``anchor`` so the first run is scheduled.
        """
        now = DateTime.utcnow()
        if now < self._grid_slot(anchor, 1):
            return anchor
        return self._align_to_prev(now - self._interval, anchor)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # No anchor available here, so fall back to the delta semantic used by
        # DeltaDataIntervalTimetable: the interval ends exactly at run_after.
        return DataInterval(start=run_after - self._interval, end=run_after)

    def next_dagrun_info(
            self,
            *,
            last_automated_data_interval: DataInterval | None,
            restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        anchor = restriction.earliest  # = DAG.start_date
        if anchor is None:
            return None

        # catchup=True: iterate slot-by-slot from start_date.
        # catchup=False: skip straight to the most-recently-completed slot.
        if restriction.catchup:
            earliest = anchor
        else:
            earliest = self._skip_to_latest(anchor)

        if last_automated_data_interval is None:
            start = earliest
        else:
            # Re-anchor the previous run's end onto the current grid -- this
            # is what lets a start_date edit shift the schedule without
            # clearing DAG history.
            align_last_end = self._align_to_prev(
                last_automated_data_interval.end, anchor
            )
            start = max(align_last_end, earliest)

        if restriction.latest is not None and start > restriction.latest:
            return None
        # Advance one interval on the wall-clock grid (not absolute seconds) so the
        # interval end holds local time across a DST transition. ``start`` is always
        # a grid slot, so this lands on the next slot and intervals stay contiguous.
        end = self._localize(start.naive() + self._interval, anchor.timezone)
        return DagRunInfo.interval(start=start, end=end)

"""Airflow plugin registrations for unic-dag."""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from timetables.interval import IntervalTimetable


class UnicPlugins(AirflowPlugin):
    """Registers custom timetables so Airflow can deserialize them from the metadata DB."""

    name = "unic_plugins"
    timetables = [IntervalTimetable]

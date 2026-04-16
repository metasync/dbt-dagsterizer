from __future__ import annotations

import os

from dagster import DailyPartitionsDefinition

_daily_partitions_def = None


def get_daily_partitions_def() -> DailyPartitionsDefinition:
    global _daily_partitions_def
    if _daily_partitions_def is not None:
        return _daily_partitions_def
    start_date = os.getenv("DAGSTER_DAILY_PARTITIONS_START_DATE")
    if not start_date:
        raise ValueError(
            "DAGSTER_DAILY_PARTITIONS_START_DATE must be set (YYYY-MM-DD) when using daily partitions"
        )
    _daily_partitions_def = DailyPartitionsDefinition(start_date=start_date)
    return _daily_partitions_def

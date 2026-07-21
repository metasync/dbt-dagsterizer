from __future__ import annotations

from datetime import datetime

from dbt_dagsterizer.assets.dbt.vars import _get_dbt_vars_for_context


class _Window:
    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end


class _PartitionedContext:
    def __init__(self, start: datetime, end: datetime):
        self.partition_time_window = _Window(start, end)


class _MissingPartitionKeyContext:
    @property
    def partition_time_window(self):
        raise RuntimeError("Failure condition: Has a PartitionsDefinition, so should either have a partition key")


def test_get_dbt_vars_for_context_from_partition_time_window():
    ctx = _PartitionedContext(datetime(2026, 1, 2, 0, 0, 0), datetime(2026, 1, 3, 0, 0, 0))
    vars_ = _get_dbt_vars_for_context(ctx)

    assert vars_ is not None
    assert vars_["min_date"] == "2026-01-02"
    assert vars_["max_date"] == "2026-01-03"
    assert vars_["min_datetime"] == "2026-01-02 00:00:00"
    assert vars_["max_datetime"] == "2026-01-03 00:00:00"


def test_get_dbt_vars_for_context_missing_partition_key_uses_default_window():
    vars_ = _get_dbt_vars_for_context(_MissingPartitionKeyContext())
    assert vars_ is not None
    assert {"min_date", "max_date", "min_datetime", "max_datetime"} <= set(vars_.keys())

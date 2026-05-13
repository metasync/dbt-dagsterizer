from __future__ import annotations


def test_daily_at_defaults_to_yesterday_partition():
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(name="my_daily", job_name="my_job", hour=0, minute=0)
    assert spec["partition_type"] == "daily"
    assert spec["partition_offset_days"] == 1


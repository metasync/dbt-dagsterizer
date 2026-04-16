from datetime import datetime, timedelta


def _dbt_partition_vars_from_time_window(start: datetime, end: datetime) -> dict[str, str]:
    min_date = start.date().strftime("%Y-%m-%d")
    max_date = end.date().strftime("%Y-%m-%d")
    min_datetime = start.strftime("%Y-%m-%d %H:%M:%S")
    max_datetime = end.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "min_date": min_date,
        "max_date": max_date,
        "min_datetime": min_datetime,
        "max_datetime": max_datetime,
    }


def _default_daily_window_vars() -> dict[str, str]:
    # Fallback window for manual/non-partitioned Dagster runs against daily-partitioned assets.
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return _dbt_partition_vars_from_time_window(start, end)


def _get_dbt_vars_for_context(context) -> dict[str, str] | None:
    try:
        time_window = context.partition_time_window
    except AttributeError:
        return None
    except Exception as e:
        if "Has a PartitionsDefinition" in str(e):
            return _default_daily_window_vars()
        raise

    if time_window is None:
        return None

    return _dbt_partition_vars_from_time_window(time_window.start, time_window.end)

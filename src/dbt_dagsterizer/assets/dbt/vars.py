from datetime import datetime


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


def _get_dbt_vars_for_context(context) -> dict[str, str] | None:
    try:
        from dagster_shared.check.functions import CheckError
    except Exception:
        CheckError = None

    try:
        time_window = context.partition_time_window
    except AttributeError:
        return None
    except Exception as e:
        if CheckError is not None and isinstance(e, CheckError):
            return None
        raise
        return None

    if time_window is None:
        return None

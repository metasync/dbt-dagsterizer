def get_dbt_schedules():
    from .schedules import get_dbt_schedules as _get_dbt_schedules

    return _get_dbt_schedules()


__all__ = ["get_dbt_schedules"]

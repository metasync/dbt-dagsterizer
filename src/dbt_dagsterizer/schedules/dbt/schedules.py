from ...jobs.dbt.jobs import get_dbt_jobs_by_name
from ...partitions_registry import get_dynamic_partitions_defs
from ...resources.dbt import get_dbt_project_dir
from ..dbt_config import DBT_SCHEDULE_SPECS
from .auto_config import build_auto_dbt_schedule_specs
from .factory import build_dbt_schedules

_dbt_schedules = None


def get_dbt_schedules():
    global _dbt_schedules
    if _dbt_schedules is None:
        dbt_project_dir = get_dbt_project_dir()
        dynamic_partitions_defs = get_dynamic_partitions_defs(dbt_project_dir)
        _dbt_schedules = build_dbt_schedules(
            build_auto_dbt_schedule_specs() + DBT_SCHEDULE_SPECS,
            get_dbt_jobs_by_name(),
            dynamic_partitions_defs=dynamic_partitions_defs,
        )
    return _dbt_schedules

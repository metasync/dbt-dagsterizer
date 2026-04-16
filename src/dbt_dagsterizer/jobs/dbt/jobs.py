from ..dbt_config import DBT_JOB_SPECS
from .auto_config import build_auto_dbt_job_specs
from .factory import build_dbt_asset_jobs

_dbt_jobs_by_name = None


def get_dbt_jobs_by_name():
    global _dbt_jobs_by_name
    if _dbt_jobs_by_name is None:
        _dbt_jobs_by_name = build_dbt_asset_jobs(build_auto_dbt_job_specs() + DBT_JOB_SPECS)
    return _dbt_jobs_by_name


def get_dbt_jobs():
    return list(get_dbt_jobs_by_name().values())

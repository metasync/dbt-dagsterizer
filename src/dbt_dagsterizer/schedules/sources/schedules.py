import os
from pathlib import Path

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ...orchestration_config import (
    default_orchestration_path,
    normalize_timezone,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir


def _get_global_timezone() -> str:
    """Read the global timezone from dagsterization.yml, defaulting to UTC."""
    try:
        dbt_project_dir = get_dbt_project_dir()
        cfg_path = resolve_orchestration_path(
            dbt_project_dir=dbt_project_dir,
            path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
        )
        cfg = load_orch(cfg_path)
        return normalize_timezone(cfg.get("timezone"), default="UTC")
    except Exception:
        return "UTC"


def get_observe_sources_schedules():
    from ...assets.sources.automation import load_filtered_observable_sources
    from ...jobs.sources.jobs import observe_job_names, resolve_observe_batch_size

    batch_size = resolve_observe_batch_size()
    job_names = observe_job_names(
        asset_count=len(load_filtered_observable_sources()),
        batch_size=batch_size,
    )
    if not job_names:
        return []

    cron_schedule = os.getenv("LUBAN_OBSERVE_SOURCES_CRON", "*/5 * * * *")
    execution_timezone = _get_global_timezone()
    return [
        ScheduleDefinition(
            name=(
                "observe_sources_schedule"
                if len(job_names) == 1
                else f"observe_sources_schedule_{i}"
            ),
            cron_schedule=cron_schedule,
            job_name=job_name,
            default_status=DefaultScheduleStatus.RUNNING,
            execution_timezone=execution_timezone,
        )
        for i, job_name in enumerate(job_names, start=1)
    ]

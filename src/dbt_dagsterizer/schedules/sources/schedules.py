import os

from dagster import DefaultScheduleStatus, ScheduleDefinition


def get_observe_sources_schedule():
    from ...assets.sources.automation import load_automation_observable_sources

    if not load_automation_observable_sources():
        return None
    return ScheduleDefinition(
        name="observe_sources_schedule",
        cron_schedule=os.getenv("LUBAN_OBSERVE_SOURCES_CRON", "*/5 * * * *"),
        job_name="observe_sources_job",
        default_status=DefaultScheduleStatus.RUNNING,
    )

import os
from itertools import batched

from dagster import define_asset_job

from ...k8s_tags import with_luban_run_k8s_config_tag


def resolve_observe_batch_size() -> int:
    """Return the validated ``LUBAN_OBSERVE_SOURCES_BATCH_SIZE`` (default 5)."""
    try:
        batch_size = int(os.getenv("LUBAN_OBSERVE_SOURCES_BATCH_SIZE", "5"))
    except ValueError:
        raise ValueError("LUBAN_OBSERVE_SOURCES_BATCH_SIZE must be a positive integer") from None
    if batch_size <= 0:
        raise ValueError("LUBAN_OBSERVE_SOURCES_BATCH_SIZE must be a positive integer")
    return batch_size


def observe_job_names(*, asset_count: int, batch_size: int) -> list[str]:
    """Names of the observation jobs produced for ``asset_count`` observable sources.

    Kept independent of the job definitions so schedules can reference the job
    names without rebuilding the observable source assets a second time.
    """
    if asset_count == 0:
        return []
    if asset_count <= batch_size:
        return ["observe_sources_job"]
    batch_count = -(-asset_count // batch_size)
    return [f"observe_sources_job_{i}" for i in range(1, batch_count + 1)]


def get_observe_sources_jobs():
    from ...assets.dbt.assets import get_dbt_assets
    from ...assets.sources.automation import load_filtered_observable_sources
    from ...assets.sources.factory import build_observable_source_assets

    batch_size = resolve_observe_batch_size()

    observable_source_assets = build_observable_source_assets(
        dbt_assets=get_dbt_assets(),
        source_specs=load_filtered_observable_sources(),
    )

    names = observe_job_names(
        asset_count=len(observable_source_assets),
        batch_size=batch_size,
    )
    return [
        define_asset_job(
            name=name,
            selection=list(batch),
            tags=with_luban_run_k8s_config_tag(None),
        )
        for name, batch in zip(names, batched(observable_source_assets, batch_size), strict=True)
    ]

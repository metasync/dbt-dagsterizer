from __future__ import annotations

from pathlib import Path

from ...dbt.manifest import iter_models, load_manifest
from ...orchestration_config import (
    default_orchestration_path,
    derive_job_name_for_model,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    index as index_orch,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir
from .presets import models_job


def build_auto_dbt_job_specs() -> list[dict]:
    manifest = load_manifest()
    existing_models = {m.name for m in iter_models(manifest)}

    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)

    specs: list[dict] = []

    jobs = cfg.get("jobs")
    if isinstance(jobs, dict):
        for job_name in sorted(jobs.keys()):
            job_cfg = jobs.get(job_name)
            if not isinstance(job_name, str) or not job_name.strip() or not isinstance(job_cfg, dict):
                continue
            models_list = [m for m in job_cfg.get("models", []) if isinstance(m, str) and m.strip()]
            models_list = sorted(set([m.strip() for m in models_list]))
            for m in models_list:
                if m not in existing_models:
                    raise ValueError(f"Orchestration job '{job_name}' references missing dbt model '{m}'")

            include_upstream = bool(job_cfg.get("include_upstream", False))
            partitions = job_cfg.get("partitions")
            partitions_value = None
            if partitions is not None:
                if partitions not in {"daily", "unpartitioned"}:
                    raise ValueError(f"Orchestration job '{job_name}' partitions must be daily|unpartitioned")
                partitions_value = partitions
            else:
                inferred = {idx.partitions_by_model.get(m) for m in models_list}
                inferred.discard(None)
                if len(inferred) == 1:
                    partitions_value = inferred.pop()

            specs.append(
                models_job(
                    name=str(job_name),
                    models=models_list,
                    include_upstream=include_upstream,
                    partitions=partitions_value,
                )
            )

    for model in sorted(idx.asset_job_models):
        if model not in existing_models:
            raise ValueError(f"Orchestration asset_jobs references missing dbt model '{model}'")
        partitions_value = idx.partitions_by_model.get(model)
        specs.append(
            models_job(
                name=str(derive_job_name_for_model(idx, model=model)),
                models=[model],
                include_upstream=False,
                partitions=partitions_value,
            )
        )

    return specs

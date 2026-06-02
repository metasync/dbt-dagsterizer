from __future__ import annotations

from pathlib import Path

from ...assets.dbt.translator import LubanDagsterDbtTranslator
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
    model_props_by_name: dict[str, dict] = {}
    for props in (manifest.get("nodes") or {}).values():
        if not isinstance(props, dict):
            continue
        if props.get("resource_type") != "model":
            continue
        name = props.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        name = name.strip()
        model_props_by_name[name] = props

    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)
    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables=set(),
        partitions_by_model=idx.partitions_by_model,
    )

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

            keys: list[list[str]] = []
            for m in models_list:
                props = model_props_by_name.get(m)
                if props is None:
                    raise ValueError(f"Could not resolve dbt model in manifest for job '{job_name}': '{m}'")
                keys.append(list(translator.get_asset_key(props).path))

            specs.append(
                models_job(
                    name=str(job_name),
                    models=models_list,
                    keys=keys,
                    include_upstream=include_upstream,
                    partitions=partitions_value,
                )
            )

    for model in sorted(idx.asset_job_models):
        if model not in existing_models:
            raise ValueError(f"Orchestration asset_jobs references missing dbt model '{model}'")
        partitions_value = idx.partitions_by_model.get(model)
        props = model_props_by_name.get(model)
        if props is None:
            raise ValueError(f"Could not resolve dbt model in manifest for asset job '{model}'")
        key = list(translator.get_asset_key(props).path)
        specs.append(
            models_job(
                name=str(derive_job_name_for_model(idx, model=model)),
                models=[model],
                keys=[key],
                include_upstream=False,
                partitions=partitions_value,
            )
        )

    return specs

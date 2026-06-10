from ...assets.dbt.translator import relation_asset_key_path
from ...dbt.manifest import iter_models, load_manifest
from ..dbt_config import DBT_JOB_SPECS
from .auto_config import build_auto_dbt_job_specs
from .factory import build_dbt_asset_jobs

_dbt_jobs_by_name = None


def _build_model_relation_index() -> dict[str, list[str]]:
    manifest = load_manifest()
    return {
        m.name: relation_asset_key_path(
            database=m.database,
            schema=m.schema,
            identifier=m.identifier,
        )
        for m in iter_models(manifest)
    }


def _normalize_manual_job_specs(job_specs: list[dict]) -> list[dict]:
    model_relations = _build_model_relation_index()
    normalized_specs: list[dict] = []

    for spec in job_specs:
        job_type = spec.get("type", "asset")
        selection = spec.get("selection")
        if job_type != "asset" or not isinstance(selection, dict) or selection.get("type") != "asset_keys":
            normalized_specs.append(spec)
            continue

        keys = selection.get("keys")
        if not isinstance(keys, list):
            normalized_specs.append(spec)
            continue

        normalized_keys: list[list[str]] = []
        changed = False
        for key in keys:
            if (
                isinstance(key, list)
                and len(key) == 2
                and key[0] == "dbt"
                and isinstance(key[1], str)
                and key[1] in model_relations
            ):
                normalized_keys.append(model_relations[key[1]])
                changed = True
            else:
                normalized_keys.append(key)

        if not changed:
            normalized_specs.append(spec)
            continue

        spec_copy = dict(spec)
        selection_copy = dict(selection)
        selection_copy["keys"] = normalized_keys
        spec_copy["selection"] = selection_copy
        normalized_specs.append(spec_copy)

    return normalized_specs


def get_dbt_jobs_by_name():
    global _dbt_jobs_by_name
    if _dbt_jobs_by_name is None:
        manual_specs = _normalize_manual_job_specs(DBT_JOB_SPECS)
        _dbt_jobs_by_name = build_dbt_asset_jobs(build_auto_dbt_job_specs() + manual_specs)
    return _dbt_jobs_by_name


def get_dbt_jobs():
    return list(get_dbt_jobs_by_name().values())

from __future__ import annotations

import os

from ...dbt.manifest_prepare import ensure_manifest
from ...resources.dbt import get_dbt_profiles_dir, get_dbt_project_dir


def prepare_manifest_if_missing() -> None:
    enabled = os.getenv("LUBAN_DBT_PREPARE_ON_LOAD", "1").strip().lower() in {"1", "true", "yes"}
    if not enabled:
        return

    project_dir = get_dbt_project_dir()
    profiles_dir = get_dbt_profiles_dir()
    target = os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"
    ensure_manifest(dbt_project_dir=project_dir, dbt_profiles_dir=profiles_dir, dbt_target=target)

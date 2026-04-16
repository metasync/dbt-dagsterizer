from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..dbt.manifest_prepare import ensure_manifest
from ..resources.dbt import get_dbt_profiles_dir, get_dbt_project_dir


@dataclass(frozen=True)
class DbtModel:
    name: str
    fqn: list[str]
    tags: set[str]
    meta: dict[str, Any]


def _manifest_path() -> Path:
    return get_dbt_project_dir() / "target" / "manifest.json"


def load_manifest() -> dict[str, Any]:
    target = os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"
    ensure_manifest(
        dbt_project_dir=get_dbt_project_dir(),
        dbt_profiles_dir=get_dbt_profiles_dir(),
        dbt_target=target,
    )
    path = _manifest_path()
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def iter_models(manifest: dict[str, Any]) -> list[DbtModel]:
    nodes = manifest.get("nodes") or {}
    result: list[DbtModel] = []
    for props in nodes.values():
        if not isinstance(props, dict):
            continue
        if props.get("resource_type") != "model":
            continue

        name = props.get("name")
        if not name:
            continue

        result.append(
            DbtModel(
                name=str(name),
                fqn=list(props.get("fqn") or []),
                tags=set(props.get("tags") or []),
                meta=dict(props.get("meta") or {}),
            )
        )
    return result


def first_tag_value(tags: set[str], *, prefix: str) -> str | None:
    for tag in tags:
        if not isinstance(tag, str):
            continue
        if tag.startswith(prefix):
            return tag[len(prefix) :]
    return None


def get_luban_meta(meta: dict[str, Any]) -> dict[str, Any]:
    luban = meta.get("luban")
    return luban if isinstance(luban, dict) else {}


def get_luban_partition(meta: dict[str, Any]) -> str | None:
    luban = get_luban_meta(meta)
    value = luban.get("partition")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def get_luban_asset_job(meta: dict[str, Any]) -> bool:
    luban = get_luban_meta(meta)
    value = luban.get("asset_job")
    return bool(value) if isinstance(value, bool) else False

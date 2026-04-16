from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .env_utils import dotenv_paths_for_dbt_project, max_mtime


@dataclass(frozen=True)
class ManifestInputs:
    version: int
    generated_at: str
    dbt_target: str
    dotenv_paths: list[str]
    dotenv_mtime_max: float | None


def manifest_inputs_path(*, dbt_project_dir: Path) -> Path:
    return dbt_project_dir / "target" / ".luban_manifest_inputs.json"


def current_manifest_inputs(*, dbt_project_dir: Path, dbt_target: str) -> ManifestInputs:
    paths = dotenv_paths_for_dbt_project(dbt_project_dir)
    return ManifestInputs(
        version=1,
        generated_at=datetime.now(timezone.utc).isoformat(),
        dbt_target=dbt_target,
        dotenv_paths=[str(p) for p in paths],
        dotenv_mtime_max=max_mtime(paths),
    )


def load_manifest_inputs(*, dbt_project_dir: Path) -> ManifestInputs | None:
    path = manifest_inputs_path(dbt_project_dir=dbt_project_dir)
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return None
        return ManifestInputs(
            version=int(obj.get("version", 0)),
            generated_at=str(obj.get("generated_at", "")),
            dbt_target=str(obj.get("dbt_target", "")),
            dotenv_paths=list(obj.get("dotenv_paths", [])),
            dotenv_mtime_max=obj.get("dotenv_mtime_max"),
        )
    except Exception:
        return None


def write_manifest_inputs(*, dbt_project_dir: Path, inputs: ManifestInputs) -> None:
    path = manifest_inputs_path(dbt_project_dir=dbt_project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": inputs.version,
        "generated_at": inputs.generated_at,
        "dbt_target": inputs.dbt_target,
        "dotenv_paths": inputs.dotenv_paths,
        "dotenv_mtime_max": inputs.dotenv_mtime_max,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def should_refresh_manifest(
    *,
    dbt_project_dir: Path,
    dbt_target: str,
    manifest_path: Path,
) -> bool:
    if not manifest_path.exists():
        return True
    saved = load_manifest_inputs(dbt_project_dir=dbt_project_dir)
    if saved is None or saved.version != 1:
        return True
    if saved.dbt_target != dbt_target:
        return True

    cur = current_manifest_inputs(dbt_project_dir=dbt_project_dir, dbt_target=dbt_target)
    if cur.dotenv_paths != saved.dotenv_paths:
        return True
    if cur.dotenv_mtime_max is None and saved.dotenv_mtime_max is None:
        return False
    if cur.dotenv_mtime_max is None and saved.dotenv_mtime_max is not None:
        return True
    if cur.dotenv_mtime_max is not None and saved.dotenv_mtime_max is None:
        return True
    return float(cur.dotenv_mtime_max) > float(saved.dotenv_mtime_max)

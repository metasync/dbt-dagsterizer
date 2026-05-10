from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dagster_dbt import DbtCliResource

from ..env_utils import dotenv_overrides_for_dbt_project, temporary_env
from ..manifest_inputs import (
    current_manifest_inputs,
    should_refresh_manifest,
    write_manifest_inputs,
)
from ..otel import otel_child_span


def manifest_path(dbt_project_dir: Path) -> Path:
    return dbt_project_dir / "target" / "manifest.json"


def _should_run_deps(project_dir: Path) -> bool:
    packages_yml = project_dir / "packages.yml"
    if not packages_yml.exists():
        return False
    text = packages_yml.read_text(encoding="utf-8")
    return any(line.lstrip().startswith("-") for line in text.splitlines())


def run_dbt_parse(*, dbt_project_dir: Path, dbt_profiles_dir: Path, dbt_target: str) -> None:
    env = {
        "DBT_PROJECT_DIR": str(dbt_project_dir),
        "DBT_PROFILES_DIR": str(dbt_profiles_dir),
    }
    env.update(dotenv_overrides_for_dbt_project(dbt_project_dir=dbt_project_dir))
    with otel_child_span(
        "dbt.manifest.prepare",
        attributes={"dbt.target": dbt_target},
    ):
        with temporary_env(env):
            target_path = dbt_project_dir / "target"
            target_path.mkdir(parents=True, exist_ok=True)
            cli = DbtCliResource(
                project_dir=str(dbt_project_dir),
                profiles_dir=str(dbt_profiles_dir),
                target=dbt_target,
            )
            if _should_run_deps(dbt_project_dir):
                cli.cli(["deps", "--quiet"], target_path=target_path).wait()
            cli.cli(["parse", "--quiet"], target_path=target_path).wait()
            write_manifest_inputs(
                dbt_project_dir=dbt_project_dir,
                inputs=current_manifest_inputs(dbt_project_dir=dbt_project_dir, dbt_target=dbt_target),
            )


def ensure_manifest(*, dbt_project_dir: Path, dbt_profiles_dir: Path, dbt_target: str) -> Path:
    path = manifest_path(dbt_project_dir)
    if should_refresh_manifest(dbt_project_dir=dbt_project_dir, dbt_target=dbt_target, manifest_path=path):
        run_dbt_parse(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir, dbt_target=dbt_target)
    return path


def load_manifest(*, dbt_project_dir: Path, dbt_profiles_dir: Path, dbt_target: str, prepare: bool) -> dict[str, Any]:
    path = manifest_path(dbt_project_dir)
    if prepare:
        path = ensure_manifest(dbt_project_dir=dbt_project_dir, dbt_profiles_dir=dbt_profiles_dir, dbt_target=dbt_target)
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

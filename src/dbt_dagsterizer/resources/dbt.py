from __future__ import annotations

import os
from pathlib import Path

from dagster_dbt import DbtCliResource


def _find_repo_root_from_cwd() -> Path | None:
    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        if (candidate / "dbt_project" / "dbt_project.yml").exists():
            return candidate
    return None


def get_repo_root() -> Path:
    root = os.getenv("LUBAN_REPO_ROOT")
    if root:
        return Path(root).expanduser().resolve()
    found = _find_repo_root_from_cwd()
    if found is not None:
        return found
    return Path(__file__).resolve().parents[3]


def get_dbt_project_dir() -> Path:
    project_dir = os.getenv("DBT_PROJECT_DIR")
    if not project_dir:
        path = get_repo_root() / "dbt_project"
        if not (path / "dbt_project.yml").exists():
            raise RuntimeError(
                "Unable to locate dbt_project.yml. Set LUBAN_REPO_ROOT to the repository root "
                "or set DBT_PROJECT_DIR to the dbt project directory."
            )
        return path

    path = Path(project_dir).expanduser()
    if path.is_absolute():
        resolved = path.resolve()
        if not (resolved / "dbt_project.yml").exists():
            raise RuntimeError(
                f"DBT_PROJECT_DIR does not contain dbt_project.yml: {resolved}. "
                "Set DBT_PROJECT_DIR to a valid dbt project directory."
            )
        return resolved

    resolved = (get_repo_root() / path).resolve()
    if not (resolved / "dbt_project.yml").exists():
        raise RuntimeError(
            f"DBT_PROJECT_DIR does not contain dbt_project.yml: {resolved}. "
            "Set LUBAN_REPO_ROOT to the repository root or set DBT_PROJECT_DIR to a valid dbt project directory."
        )
    return resolved


def get_dbt_profiles_dir() -> Path:
    profiles_dir = os.getenv("DBT_PROFILES_DIR")
    if not profiles_dir:
        path = get_dbt_project_dir()
        if not (path / "profiles.yml").exists():
            raise RuntimeError(
                f"dbt profiles.yml not found at: {path / 'profiles.yml'}. "
                "Set DBT_PROFILES_DIR to a directory containing profiles.yml."
            )
        return path

    path = Path(profiles_dir).expanduser()
    if path.is_absolute():
        resolved = path.resolve()
        if not (resolved / "profiles.yml").exists():
            raise RuntimeError(
                f"DBT_PROFILES_DIR does not contain profiles.yml: {resolved}. "
                "Set DBT_PROFILES_DIR to a directory containing profiles.yml."
            )
        return resolved

    resolved = (get_repo_root() / path).resolve()
    if not (resolved / "profiles.yml").exists():
        raise RuntimeError(
            f"DBT_PROFILES_DIR does not contain profiles.yml: {resolved}. "
            "Set LUBAN_REPO_ROOT to the repository root or set DBT_PROFILES_DIR to a directory containing profiles.yml."
        )
    return resolved


def make_dbt_resource() -> DbtCliResource:
    target = os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"

    return DbtCliResource(
        project_dir=str(get_dbt_project_dir()),
        profiles_dir=str(get_dbt_profiles_dir()),
        target=target,
    )

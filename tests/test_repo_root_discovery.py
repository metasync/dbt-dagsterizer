from __future__ import annotations

import os
from pathlib import Path

from dbt_dagsterizer.resources.dbt import get_repo_root


def test_get_repo_root_discovers_parent_with_dbt_project(tmp_path: Path):
    repo_root = tmp_path / "repo"
    dbt_project_dir = repo_root / "dbt_project"
    (dbt_project_dir / "models").mkdir(parents=True)
    (dbt_project_dir / "dbt_project.yml").write_text(
        "name: demo\nprofile: demo\nconfig-version: 2\nversion: '1.0.0'\nmodel-paths: ['models']\n",
        encoding="utf-8",
    )

    cwd = repo_root / ".venv" / "lib" / "python3.12"
    cwd.mkdir(parents=True)

    old_cwd = Path.cwd()
    old_env = os.environ.get("LUBAN_REPO_ROOT")
    try:
        os.environ.pop("LUBAN_REPO_ROOT", None)
        os.chdir(cwd)
        assert get_repo_root() == repo_root.resolve()
    finally:
        os.chdir(old_cwd)
        if old_env is None:
            os.environ.pop("LUBAN_REPO_ROOT", None)
        else:
            os.environ["LUBAN_REPO_ROOT"] = old_env


def test_get_dbt_project_dir_errors_when_not_discoverable(tmp_path: Path):
    from dbt_dagsterizer.resources.dbt import get_dbt_project_dir

    cwd = tmp_path / "somewhere" / "deep"
    cwd.mkdir(parents=True)

    old_cwd = Path.cwd()
    old_repo_root = os.environ.get("LUBAN_REPO_ROOT")
    old_project_dir = os.environ.get("DBT_PROJECT_DIR")
    try:
        os.environ.pop("LUBAN_REPO_ROOT", None)
        os.environ.pop("DBT_PROJECT_DIR", None)
        os.chdir(cwd)
        try:
            get_dbt_project_dir()
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "LUBAN_REPO_ROOT" in str(e) or "DBT_PROJECT_DIR" in str(e)
    finally:
        os.chdir(old_cwd)
        if old_repo_root is None:
            os.environ.pop("LUBAN_REPO_ROOT", None)
        else:
            os.environ["LUBAN_REPO_ROOT"] = old_repo_root
        if old_project_dir is None:
            os.environ.pop("DBT_PROJECT_DIR", None)
        else:
            os.environ["DBT_PROJECT_DIR"] = old_project_dir

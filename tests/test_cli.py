from pathlib import Path

from dbt_dagsterizer.cli_parts.common import resolve_dir_arg
from dbt_dagsterizer.cli_parts.macros import _DEFAULT_TEMPLATE_NAME, _sync_macros
from dbt_dagsterizer.cli_parts.validation import (
    validate_orchestration,
    validate_orchestration_structure,
)


def test_resolve_dir_arg_relative(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert resolve_dir_arg("dbt_project") == (tmp_path / "dbt_project").resolve()


def test_sync_macros(tmp_path: Path):
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    synced, macros_dir = _sync_macros(
        dbt_project_path=dbt_project, template_name=_DEFAULT_TEMPLATE_NAME, force=False
    )
    assert macros_dir.exists()
    assert synced > 0
    assert any(macros_dir.rglob("*.sql"))


def test_validate_manifest_empty():
    issues = validate_orchestration(
        manifest={"nodes": {}, "sources": {}},
        orchestration={"version": 1, "jobs": {}, "asset_jobs": [], "partitions": {}, "schedules": {}, "partition_change": {"detectors": [], "propagators": []}},
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    assert issues == []


def test_validate_structure_duplicate_model_in_jobs():
    issues = validate_orchestration_structure(
        orchestration={
            "version": 1,
            "jobs": {
                "j1": {"models": ["m1"], "include_upstream": False},
                "j2": {"models": ["m1"], "include_upstream": False},
            },
            "asset_jobs": [],
            "partitions": {},
            "schedules": {},
            "partition_change": {"detectors": [], "propagators": []},
        }
    )
    assert any(i.level == "error" for i in issues)

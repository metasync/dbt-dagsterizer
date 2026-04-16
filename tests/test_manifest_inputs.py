from __future__ import annotations

from pathlib import Path

from dbt_dagsterizer.manifest_inputs import (
    current_manifest_inputs,
    should_refresh_manifest,
    write_manifest_inputs,
)


def test_should_refresh_manifest_when_signature_missing(tmp_path: Path):
    dbt_project = tmp_path / "dbt_project"
    (dbt_project / "target").mkdir(parents=True)
    manifest = dbt_project / "target" / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    assert should_refresh_manifest(dbt_project_dir=dbt_project, dbt_target="development", manifest_path=manifest)


def test_should_not_refresh_when_signature_present_and_unchanged(tmp_path: Path):
    dbt_project = tmp_path / "dbt_project"
    (dbt_project / "target").mkdir(parents=True)
    manifest = dbt_project / "target" / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    inputs = current_manifest_inputs(dbt_project_dir=dbt_project, dbt_target="development")
    write_manifest_inputs(dbt_project_dir=dbt_project, inputs=inputs)

    assert not should_refresh_manifest(
        dbt_project_dir=dbt_project,
        dbt_target="development",
        manifest_path=manifest,
    )


def test_should_refresh_when_dotenv_removed(tmp_path: Path):
    dbt_project = tmp_path / "dbt_project"
    (dbt_project / "target").mkdir(parents=True)
    manifest = dbt_project / "target" / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("STARROCKS_ODS_DB=myods\n", encoding="utf-8")
    write_manifest_inputs(
        dbt_project_dir=dbt_project,
        inputs=current_manifest_inputs(dbt_project_dir=dbt_project, dbt_target="development"),
    )
    dotenv_path.unlink()

    assert should_refresh_manifest(dbt_project_dir=dbt_project, dbt_target="development", manifest_path=manifest)

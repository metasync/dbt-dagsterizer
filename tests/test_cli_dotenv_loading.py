from __future__ import annotations

import json
import os
from pathlib import Path


def test_run_dbt_parse_loads_dotenv(tmp_path: Path, monkeypatch):
    from dbt_dagsterizer.dbt import manifest_prepare

    monkeypatch.delenv("STARROCKS_ODS_DB", raising=False)

    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)
    (tmp_path / ".env").write_text("STARROCKS_ODS_DB=myods\n", encoding="utf-8")

    called = {"count": 0}

    class _DummyCli:
        def __init__(self, **_kw):
            assert os.environ.get("STARROCKS_ODS_DB") == "myods"
            called["count"] += 1

        def cli(self, _args, target_path):
            (Path(target_path) / "manifest.json").write_text(json.dumps({}), encoding="utf-8")

            class _W:
                def wait(self):
                    return None

            return _W()

    monkeypatch.setattr(manifest_prepare, "DbtCliResource", lambda **kw: _DummyCli(**kw))

    manifest_prepare.run_dbt_parse(
        dbt_project_dir=dbt_project,
        dbt_profiles_dir=dbt_project,
        dbt_target="development",
    )

    assert called["count"] == 1
    assert (dbt_project / "target" / "manifest.json").exists()
    assert (dbt_project / "target" / ".luban_manifest_inputs.json").exists()


def test_run_dbt_parse_does_not_override_existing_env(tmp_path: Path, monkeypatch):
    from dbt_dagsterizer.dbt import manifest_prepare

    monkeypatch.setenv("STARROCKS_ODS_DB", "existing")

    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)
    (tmp_path / ".env").write_text("STARROCKS_ODS_DB=myods\n", encoding="utf-8")

    class _DummyCli:
        def __init__(self, **_kw):
            assert os.environ.get("STARROCKS_ODS_DB") == "existing"

        def cli(self, _args, target_path):
            (Path(target_path) / "manifest.json").write_text(json.dumps({}), encoding="utf-8")

            class _W:
                def wait(self):
                    return None

            return _W()

    monkeypatch.setattr(manifest_prepare, "DbtCliResource", lambda **kw: _DummyCli(**kw))

    manifest_prepare.run_dbt_parse(
        dbt_project_dir=dbt_project,
        dbt_profiles_dir=dbt_project,
        dbt_target="development",
    )


def test_ensure_manifest_refreshes_when_dotenv_newer(tmp_path: Path, monkeypatch):
    from dbt_dagsterizer.dbt import manifest_prepare
    from dbt_dagsterizer.manifest_inputs import current_manifest_inputs, write_manifest_inputs

    dbt_project = tmp_path / "dbt_project"
    (dbt_project / "target").mkdir(parents=True)

    manifest = dbt_project / "target" / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    write_manifest_inputs(
        dbt_project_dir=dbt_project,
        inputs=current_manifest_inputs(dbt_project_dir=dbt_project, dbt_target="development"),
    )

    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("STARROCKS_ODS_DB=myods\n", encoding="utf-8")
    os.utime(dotenv_path, None)

    called = {"count": 0}

    def _fake_parse(*, dbt_project_dir: Path, dbt_profiles_dir: Path, dbt_target: str) -> None:
        called["count"] += 1
        (dbt_project_dir / "target").mkdir(parents=True, exist_ok=True)
        (dbt_project_dir / "target" / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(manifest_prepare, "run_dbt_parse", _fake_parse)

    manifest_prepare.ensure_manifest(
        dbt_project_dir=dbt_project,
        dbt_profiles_dir=dbt_project,
        dbt_target="development",
    )
    assert called["count"] == 1


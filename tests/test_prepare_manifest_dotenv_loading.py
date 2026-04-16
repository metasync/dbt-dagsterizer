from __future__ import annotations

import os
from pathlib import Path


def test_prepare_manifest_loads_project_root_dotenv(tmp_path: Path, monkeypatch):
    from dbt_dagsterizer.assets.dbt import prepare as prep
    from dbt_dagsterizer.dbt import manifest_prepare

    monkeypatch.delenv("STARROCKS_ODS_DB", raising=False)

    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir(parents=True)
    (tmp_path / ".env").write_text("STARROCKS_ODS_DB=myods\n", encoding="utf-8")

    monkeypatch.setattr(prep, "get_dbt_project_dir", lambda: project_dir)
    monkeypatch.setattr(prep, "get_dbt_profiles_dir", lambda: project_dir)

    (project_dir / "target").mkdir(parents=True, exist_ok=True)

    class _DummyCli:
        def cli(self, _args, target_path):
            (Path(target_path) / "manifest.json").write_text("{}", encoding="utf-8")

            class _W:
                def wait(self):
                    return None

            return _W()

    def _resource_factory(**_kw):
        assert os.environ.get("STARROCKS_ODS_DB") == "myods"
        return _DummyCli()

    monkeypatch.setattr(manifest_prepare, "DbtCliResource", _resource_factory)

    prep.prepare_manifest_if_missing()
    assert (project_dir / "target" / "manifest.json").exists()

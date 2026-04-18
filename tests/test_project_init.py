from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from dbt_dagsterizer.cli import cli


def test_project_init_renders_into_output_dir(tmp_path: Path):
    runner = CliRunner()
    out_dir = tmp_path / "out"

    result = runner.invoke(
        cli,
        [
            "project",
            "init",
            "--output-dir",
            str(out_dir),
            "--project-name",
            "Demo App",
            "--author-name",
            "Demo",
            "--author-email",
            "demo@example.com",
        ],
    )
    assert result.exit_code == 0, result.output
    project_root = out_dir / "demo_app"
    assert project_root.exists()
    assert (project_root / "pyproject.toml").exists()
    assert (project_root / "dbt_project" / "dbt_project.yml").exists()
    assert list((project_root / "dbt_project" / "models").rglob("*.sql")) == []
    assert (project_root / "dbt_project" / ".dbt_dagsterizer_template").exists()
    assert (project_root / "dbt_project" / "macros" / "dbt_dagsterizer").exists()
    assert any((project_root / "dbt_project" / "macros" / "dbt_dagsterizer").rglob("*.sql"))

    env_example = (project_root / ".env.example").read_text(encoding="utf-8")
    assert "DAGSTER_MAX_CONCURRENT_RUNS=2" in env_example
    assert f"DAGSTER_HOME={project_root / 'dagster_home'}" in env_example
    assert f"LUBAN_REPO_ROOT={project_root}" in env_example
    assert (project_root / "dagster_home" / "dagster.yaml").exists()
    assert not (project_root / "docker").exists()

    definitions_py = project_root / "src" / "demo_app" / "definitions.py"
    assert definitions_py.exists()
    content = definitions_py.read_text(encoding="utf-8")
    assert "REPO_ROOT = Path(__file__).resolve().parents[2]" in content
    assert 'os.environ.setdefault("LUBAN_REPO_ROOT", str(REPO_ROOT))' in content
    assert 'os.environ.setdefault("DBT_PROJECT_DIR", str(DBT_PROJECT_DIR))' in content
    assert 'os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))' in content
    assert "dbt_project_dir=DBT_PROJECT_DIR" in content

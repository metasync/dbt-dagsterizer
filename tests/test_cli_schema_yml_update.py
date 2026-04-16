from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner
from ruamel.yaml import YAML

from dbt_dagsterizer.cli import cli


def _load_yaml(path: Path):
    y = YAML()
    with path.open("r", encoding="utf-8") as f:
        return y.load(f)


def test_meta_job_updates_orchestration_file(tmp_path: Path):
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "meta",
            "job",
            "--dbt-project-dir",
            str(dbt_project),
            "--models",
            "fact_customer_orders_daily",
            "--name",
            "daily_facts_job",
            "--include-upstream",
            "--partitions",
            "daily",
            "--no-prepare",
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    orch = dbt_project / "dagsterization.yml"
    data = _load_yaml(orch)
    assert data["jobs"]["daily_facts_job"]["models"] == ["fact_customer_orders_daily"]
    assert data["jobs"]["daily_facts_job"]["include_upstream"] is True
    assert data["jobs"]["daily_facts_job"]["partitions"] == "daily"
    assert "fact_customer_orders_daily" in data["partitions"]["daily"]

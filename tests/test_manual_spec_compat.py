from __future__ import annotations


def test_normalize_manual_job_specs_upgrades_legacy_dbt_asset_keys(monkeypatch):
    from dbt_dagsterizer.jobs.dbt.jobs import _normalize_manual_job_specs

    monkeypatch.setattr(
        "dbt_dagsterizer.jobs.dbt.jobs._build_model_relation_index",
        lambda: {"orders": ["dbt", "warehouse", "ods", "orders"]},
    )

    specs = [
        {
            "name": "manual_orders_job",
            "selection": {
                "type": "asset_keys",
                "keys": [["dbt", "orders"]],
                "upstream": False,
            },
        }
    ]

    normalized = _normalize_manual_job_specs(specs)

    assert normalized[0]["selection"]["keys"] == [["dbt", "warehouse", "ods", "orders"]]


def test_normalize_manual_propagation_specs_injects_relation_key(monkeypatch):
    from dbt_dagsterizer.sensors import _normalize_manual_propagation_specs

    monkeypatch.setattr(
        "dbt_dagsterizer.sensors._build_model_relation_index",
        lambda: {"orders": ["dbt", "warehouse", "ods", "orders"]},
    )

    specs = [
        {
            "name": "orders_to_downstream",
            "upstream_dbt_model": "orders",
            "job_name": "downstream_job",
            "enabled": True,
        }
    ]

    normalized = _normalize_manual_propagation_specs(specs)

    assert normalized[0]["upstream_model_relation"] == ["dbt", "warehouse", "ods", "orders"]

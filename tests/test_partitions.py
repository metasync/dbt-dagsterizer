from __future__ import annotations

import pytest


def test_daily_partitions_requires_env_var(monkeypatch):
    from dbt_dagsterizer import partitions

    monkeypatch.delenv("DAGSTER_DAILY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    with pytest.raises(ValueError, match="DAGSTER_DAILY_PARTITIONS_START_DATE"):
        partitions.get_daily_partitions_def()


def test_daily_partitions_def_is_cached(monkeypatch):
    from dagster import DailyPartitionsDefinition

    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    first = partitions.get_daily_partitions_def()
    second = partitions.get_daily_partitions_def()

    assert isinstance(first, DailyPartitionsDefinition)
    assert first is second


def test_job_factory_daily_partitions_requires_env_var(monkeypatch):
    from dbt_dagsterizer import partitions
    from dbt_dagsterizer.jobs.dbt import factory as job_factory

    monkeypatch.delenv("DAGSTER_DAILY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    with pytest.raises(ValueError, match="DAGSTER_DAILY_PARTITIONS_START_DATE"):
        job_factory._get_partitions_def("daily")


def test_translator_can_lazy_load_daily_partitions_def(monkeypatch):
    from dagster import DailyPartitionsDefinition

    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")

    t = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables=set(),
        partitions_by_model={"orders": "daily"},
    )

    partitions_def = t.get_partitions_def({"name": "orders"})
    assert isinstance(partitions_def, DailyPartitionsDefinition)
    assert t.daily_partitions_def is partitions_def


@pytest.mark.parametrize(
    ("props", "partitions_by_model", "propagator_mode", "should_enable"),
    [
        (
            {"resource_type": "model", "name": "orders", "fqn": ["pkg", "staging", "orders"]},
            {},
            "sensor",
            True,
        ),
        (
            {"resource_type": "model", "name": "fact_orders", "tags": [], "fqn": ["pkg", "mart", "fact_orders"]},
            {"fact_orders": "daily"},
            "eager",
            True,
        ),
        (
            {"resource_type": "model", "name": "dim_customer", "tags": ["dim"], "fqn": ["pkg", "shared", "dim_customer"]},
            {},
            "sensor",
            True,
        ),
        (
            {
                "resource_type": "model",
                "name": "custom_model",
                "tags": ["automation_table"],
                "fqn": ["pkg", "custom", "custom_model"],
            },
            {},
            "sensor",
            True,
        ),
        (
            {"resource_type": "model", "name": "plain_model", "tags": [], "fqn": ["pkg", "custom", "plain_model"]},
            {},
            "sensor",
            False,
        ),
        (
            {
                "resource_type": "model",
                "name": "view_model",
                "tags": ["materialize_at_startup"],
                "fqn": ["pkg", "custom", "view_model"],
            },
            {},
            "sensor",
            True,
        ),
    ],
)
def test_translator_automation_rules(monkeypatch, props, partitions_by_model, propagator_mode, should_enable):
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    monkeypatch.setenv("LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE", propagator_mode)

    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables={"orders"},
        partitions_by_model=partitions_by_model,
    )

    condition = translator.get_automation_condition(props)

    if should_enable:
        assert condition is not None
    else:
        assert condition is None


def test_translator_view_materialization_is_fire_once():
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables=set(),
        partitions_by_model={},
    )

    props = {
        "resource_type": "model",
        "name": "view_model",
        "tags": ["materialize_at_startup"],
        "fqn": ["pkg", "custom", "view_model"],
    }

    condition = translator.get_automation_condition(props)

    import dagster as dg

    assert condition is not None
    assert condition == dg.AutomationCondition.missing()


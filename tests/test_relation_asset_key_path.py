from __future__ import annotations


def test_relation_asset_key_path_omits_empty_database():
    from dbt_dagsterizer.assets.dbt.translator import relation_asset_key_path

    assert relation_asset_key_path(database="", schema="mydb", identifier="customers") == ["dbt", "mydb", "customers"]


def test_relation_asset_key_path_keeps_database_and_schema_when_present():
    from dbt_dagsterizer.assets.dbt.translator import relation_asset_key_path

    assert relation_asset_key_path(database="warehouse", schema="dwd", identifier="customers") == [
        "dbt",
        "warehouse",
        "dwd",
        "customers",
    ]


def test_relation_asset_key_path_sanitizes_unsupported_characters():
    from dbt_dagsterizer.assets.dbt.translator import relation_asset_key_path

    assert relation_asset_key_path(database="my-db", schema="raw.data", identifier="orders-daily") == [
        "dbt",
        "my_db",
        "raw_data",
        "orders_daily",
    ]

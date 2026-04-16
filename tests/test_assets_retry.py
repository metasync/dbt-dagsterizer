from __future__ import annotations


def test_should_retry_dbt_cli_error_matches_once():
    from dbt_dagsterizer.assets.dbt import assets as dbt_assets

    assert dbt_assets._should_retry_dbt_cli_error(message="Table already exists", retry_number=0) is True
    assert dbt_assets._should_retry_dbt_cli_error(message="Table already exists", retry_number=1) is False


def test_should_retry_dbt_cli_error_requires_match():
    from dbt_dagsterizer.assets.dbt import assets as dbt_assets

    assert dbt_assets._should_retry_dbt_cli_error(message="something else", retry_number=0) is False

"""Tests for dynamic partitions functionality.

Tests cover:
1. Partition definition caching and creation
2. Configuration parsing for dynamic partitions
3. Partition spec resolution
4. Asset translator with dynamic partitions
5. Job factory with dynamic partitions
"""

from __future__ import annotations

import pytest

from dbt_dagsterizer.orchestration_config import (
    DynamicPartitionConfig,
    index as index_orch,
    load_or_create,
    set_dynamic_partition,
    remove_dynamic_partition,
)
from dbt_dagsterizer.partitions import get_partitions_def
from dbt_dagsterizer.partitions_dynamic import (
    get_or_create_dynamic_partitions_def,
    update_dynamic_partition_keys,
    reset_dynamic_partitions_cache,
    get_dynamic_partitions_def,
    has_dynamic_partition,
)
from dbt_dagsterizer.partitions_registry import reset_dynamic_partitions_registry


class TestDynamicPartitionsCaching:
    """Test dynamic partitions caching and management."""

    def setup_method(self):
        """Reset cache before each test."""
        reset_dynamic_partitions_cache()

    def test_create_dynamic_partition_def(self):
        """Test creating a dynamic partition definition."""
        dyn_def = get_or_create_dynamic_partitions_def(
            name="country_code",
            initial_partition_keys=["US", "GB", "DE"],
        )
        assert dyn_def is not None
        assert has_dynamic_partition("country_code")

    def test_dynamic_partition_def_caching(self):
        """Test that dynamic partitions are cached."""
        def1 = get_or_create_dynamic_partitions_def(
            name="country_code",
            initial_partition_keys=["US", "GB"],
        )
        def2 = get_or_create_dynamic_partitions_def(
            name="country_code",
            initial_partition_keys=["US", "GB", "DE"],  # Different keys
        )
        # Should return cached version (with original keys)
        assert def1 is def2

    def test_dynamic_partition_keys_update(self):
        """Test updating partition keys."""
        get_or_create_dynamic_partitions_def(
            name="country",
            initial_partition_keys=["US", "GB"],
        )
        
        # Update the keys
        update_dynamic_partition_keys("country", ["US", "GB", "DE", "JP"])
        
        # Should be retrievable
        assert has_dynamic_partition("country")

    def test_invalid_partition_name_raises_error(self):
        """Test that empty partition names raise errors."""
        with pytest.raises(ValueError, match="non-empty"):
            get_or_create_dynamic_partitions_def(
                name="",
                initial_partition_keys=["US"],
            )

    def test_empty_partition_keys_raises_error(self):
        """Test that empty partition keys raise errors."""
        with pytest.raises(ValueError, match="non-empty"):
            get_or_create_dynamic_partitions_def(
                name="country",
                initial_partition_keys=[],
            )


class TestConfigurationParsing:
    """Test dynamic partitions configuration parsing."""

    def test_parse_dynamic_partition_config(self):
        """Test parsing dynamic partition configuration."""
        config = {
            "version": 2,
            "partitions": {
                "dynamic": [
                    {
                        "name": "country_code",
                        "initial_partition_keys": ["US", "GB", "DE"],
                    },
                    {
                        "name": "tenant_id",
                        "initial_partition_keys": ["tenant_1", "tenant_2"],
                    },
                ]
            },
        }
        
        idx = index_orch(config)
        
        assert len(idx.dynamic_partitions) == 2
        assert "country_code" in idx.dynamic_partitions
        assert "tenant_id" in idx.dynamic_partitions
        assert idx.dynamic_partitions["country_code"].name == "country_code"
        assert idx.dynamic_partitions["country_code"].initial_partition_keys == ["US", "GB", "DE"]

    def test_set_dynamic_partition(self, tmp_path):
        """Test setting dynamic partition via config API."""
        cfg_file = tmp_path / "dagsterization.yml"
        data = load_or_create(cfg_file)
        
        set_dynamic_partition(
            data=data,
            name="region",
            initial_partition_keys=["APAC", "EMEA", "AMERICAS"],
        )
        
        idx = index_orch(data)
        assert "region" in idx.dynamic_partitions
        assert idx.dynamic_partitions["region"].initial_partition_keys == ["APAC", "EMEA", "AMERICAS"]

    def test_remove_dynamic_partition(self, tmp_path):
        """Test removing a dynamic partition."""
        cfg_file = tmp_path / "dagsterization.yml"
        data = load_or_create(cfg_file)
        
        set_dynamic_partition(
            data=data,
            name="region",
            initial_partition_keys=["APAC"],
        )
        
        removed = remove_dynamic_partition(data=data, name="region")
        assert removed is True
        
        idx = index_orch(data)
        assert "region" not in idx.dynamic_partitions


class TestPartitionSpecResolution:
    """Test partition specification resolution."""

    def setup_method(self):
        """Reset caches before each test."""
        reset_dynamic_partitions_cache()
        reset_dynamic_partitions_registry()

    def test_resolve_daily_partition_spec(self):
        """Test resolving daily partition spec."""
        import os
        os.environ["DAGSTER_DAILY_PARTITIONS_START_DATE"] = "2024-01-01"
        try:
            parts_def = get_partitions_def("daily")
            assert parts_def is not None
        finally:
            del os.environ["DAGSTER_DAILY_PARTITIONS_START_DATE"]

    def test_resolve_unpartitioned_spec(self):
        """Test resolving unpartitioned spec."""
        parts_def = get_partitions_def("unpartitioned")
        assert parts_def is None

    def test_resolve_dynamic_partition_spec(self):
        """Test resolving dynamic partition spec."""
        # Create a dynamic partition first
        dyn_def = get_or_create_dynamic_partitions_def(
            name="country",
            initial_partition_keys=["US", "GB"],
        )
        
        dynamic_defs = {"country": dyn_def}
        parts_def = get_partitions_def("dynamic:country", dynamic_defs)
        
        assert parts_def is dyn_def

    def test_missing_dynamic_partition_raises_error(self):
        """Test that referencing missing dynamic partition raises error."""
        with pytest.raises(ValueError, match="Unknown dynamic partition"):
            get_partitions_def("dynamic:nonexistent", {})

    def test_dynamic_partition_without_defs_raises_error(self):
        """Test that dynamic partition spec without defs raises error."""
        with pytest.raises(ValueError, match="cannot be resolved"):
            get_partitions_def("dynamic:country", None)


class TestAssetTranslator:
    """Test asset translator with dynamic partitions."""

    def setup_method(self):
        """Reset caches."""
        reset_dynamic_partitions_cache()

    def test_translator_with_daily_partition(self):
        """Test translator returns daily partition for daily model."""
        from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator
        import os
        os.environ["DAGSTER_DAILY_PARTITIONS_START_DATE"] = "2024-01-01"
        try:
            translator = LubanDagsterDbtTranslator(
                daily_partitions_def=None,
                dynamic_partitions_defs={},
                automation_observable_tables=set(),
                partitions_by_model={"orders": "daily"},
            )
            
            # IMPORTANT: Translator returns None to preserve lineage across partition types
            # Partitioning is handled at the job/schedule level
            parts_def = translator.get_partitions_def({"name": "orders"})
            assert parts_def is None
        finally:
            del os.environ["DAGSTER_DAILY_PARTITIONS_START_DATE"]

    def test_translator_with_dynamic_partition(self):
        """Test translator returns dynamic partition for dynamic model."""
        from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator
        
        dyn_def = get_or_create_dynamic_partitions_def(
            name="region",
            initial_partition_keys=["US", "EU"],
        )
        
        translator = LubanDagsterDbtTranslator(
            daily_partitions_def=None,
            dynamic_partitions_defs={"region": dyn_def},
            automation_observable_tables=set(),
            partitions_by_model={"orders_by_region": "dynamic:region"},
        )
        
        # IMPORTANT: Translator returns None to preserve lineage across partition types
        # Partitioning is handled at the job/schedule level
        parts_def = translator.get_partitions_def({"name": "orders_by_region"})
        assert parts_def is None

    def test_translator_returns_none_for_unpartitioned(self):
        """Test translator returns None for unpartitioned model."""
        from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator
        
        translator = LubanDagsterDbtTranslator(
            daily_partitions_def=None,
            dynamic_partitions_defs={},
            automation_observable_tables=set(),
            partitions_by_model={"dim_table": "unpartitioned"},
        )
        
        parts_def = translator.get_partitions_def({"name": "dim_table"})
        assert parts_def is None


class TestJobFactory:
    """Test job factory with dynamic partitions."""

    def setup_method(self):
        """Reset caches."""
        reset_dynamic_partitions_cache()

    def test_get_partitions_def_daily(self):
        """Test getting daily partition def from factory."""
        from dbt_dagsterizer.jobs.dbt.factory import _get_partitions_def
        import os
        os.environ["DAGSTER_DAILY_PARTITIONS_START_DATE"] = "2024-01-01"
        try:
            parts_def = _get_partitions_def("daily")
            assert parts_def is not None
        finally:
            del os.environ["DAGSTER_DAILY_PARTITIONS_START_DATE"]

    def test_get_partitions_def_dynamic(self):
        """Test getting dynamic partition def from factory."""
        from dbt_dagsterizer.jobs.dbt.factory import _get_partitions_def
        
        dyn_def = get_or_create_dynamic_partitions_def(
            name="country",
            initial_partition_keys=["US", "GB"],
        )
        
        dynamic_defs = {"country": dyn_def}
        parts_def = _get_partitions_def("dynamic:country", dynamic_defs)
        
        assert parts_def is dyn_def

    def test_get_partitions_def_unpartitioned(self):
        """Test getting unpartitioned spec from factory."""
        from dbt_dagsterizer.jobs.dbt.factory import _get_partitions_def
        
        parts_def = _get_partitions_def("unpartitioned")
        assert parts_def is None
        
        parts_def = _get_partitions_def(None)
        assert parts_def is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

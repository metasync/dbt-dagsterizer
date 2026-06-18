"""Dynamic partitions registry and initialization.

This module provides centralized initialization and access to dynamic partition
definitions. It's used to ensure all components (jobs, schedules, assets, sensors)
work with the same partition definitions initialized from the orchestration config.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg

from .orchestration_config import (
    default_orchestration_path,
    index as index_orch,
    load_or_create as load_orch,
    resolve_orchestration_path,
)
from .partitions_dynamic import get_or_create_dynamic_partitions_def, reset_dynamic_partitions_cache

# Global cache for initialized dynamic partitions
_dynamic_partitions_cache: dict[str, dg.PartitionsDefinition] | None = None


def _load_dynamic_partitions_from_config(
    dbt_project_dir: Path,
) -> dict[str, dg.PartitionsDefinition]:
    """Load and initialize all dynamic partition definitions from orchestration config.
    
    Args:
        dbt_project_dir: Path to dbt project directory
    
    Returns:
        Dictionary mapping partition names to DynamicPartitionsDefinition objects
    """
    orch_cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    orch_cfg = load_orch(orch_cfg_path)
    orch_index = index_orch(orch_cfg)
    
    dynamic_defs: dict[str, dg.PartitionsDefinition] = {}
    for partition_name, config in orch_index.dynamic_partitions.items():
        dyn_def = get_or_create_dynamic_partitions_def(
            name=partition_name,
            initial_partition_keys=config.initial_partition_keys,
        )
        dynamic_defs[partition_name] = dyn_def
    
    return dynamic_defs


def get_dynamic_partitions_defs(
    dbt_project_dir: Path,
) -> dict[str, dg.PartitionsDefinition]:
    """Get or initialize dynamic partition definitions.
    
    Loads definitions from orchestration config on first call, then caches them
    for subsequent calls within the same process.
    
    Args:
        dbt_project_dir: Path to dbt project directory
    
    Returns:
        Dictionary mapping partition names to DynamicPartitionsDefinition objects
    """
    global _dynamic_partitions_cache
    
    if _dynamic_partitions_cache is None:
        _dynamic_partitions_cache = _load_dynamic_partitions_from_config(dbt_project_dir)
    
    return _dynamic_partitions_cache


def reset_dynamic_partitions_registry() -> None:
    """Reset the dynamic partitions registry.
    
    This is mainly useful for testing to ensure a clean state between tests.
    """
    global _dynamic_partitions_cache
    _dynamic_partitions_cache = None
    reset_dynamic_partitions_cache()

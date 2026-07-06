"""Dynamic partitions sync sensor.

This sensor ensures dynamic partition keys in the Dagster instance always match
the initial_partition_keys from dagsterization.yml. It runs periodically and will:
1. Add any keys from config that are missing from the instance
2. Remove any keys from the instance that are not in config

This makes dagsterization.yml the single source of truth for dynamic partition keys.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg

from ..orchestration_config import (
    default_orchestration_path,
    index as index_orch,
    load_or_create as load_orch,
    resolve_orchestration_path,
)
from ..partitions_dynamic import get_dynamic_partitions_def, has_dynamic_partition


def _get_orchestration_config(dbt_project_dir: Path):
    """Load and index orchestration config."""
    orch_cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    orch_cfg = load_orch(orch_cfg_path)
    return index_orch(orch_cfg)


def build_dynamic_partitions_bootstrap_sensor(
    dbt_project_dir: Path,
    minimum_interval_seconds: int = 60,
) -> dg.SensorDefinition:
    """Build a sensor that syncs dynamic partitions from dagsterization.yml.
    
    This sensor runs periodically and ensures the Dagster instance's partition
    keys match the initial_partition_keys from dagsterization.yml. It will:
    1. Add any keys from config that are missing from the instance
    2. Remove any keys from the instance that are not in config
    
    Args:
        dbt_project_dir: Path to dbt project directory
        minimum_interval_seconds: Minimum interval between sensor evaluations
    
    Returns:
        A Dagster sensor definition
    """
    # IMPORTANT: Initialize dynamic partition definitions from config first
    # This ensures the definitions exist in the cache before the sensor runs
    from ..partitions_registry import get_dynamic_partitions_defs
    get_dynamic_partitions_defs(dbt_project_dir)
    
    # Load orchestration config at build time for closure
    orch_index = _get_orchestration_config(dbt_project_dir)

    @dg.sensor(
        name="dynamic_partitions_bootstrap_sensor",
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
        dynamic_partitions_requests = []
        
        context.log.info(f"Dynamic partitions bootstrap sensor ticking. Found {len(orch_index.dynamic_partitions)} dynamic partitions in config.")

        for partition_name, config in orch_index.dynamic_partitions.items():
            context.log.info(f"Processing dynamic partition: {partition_name}")
            
            # Get the dynamic partition definition
            if not has_dynamic_partition(partition_name):
                context.log.warning(f"Dynamic partition {partition_name} not found in cache, skipping")
                continue

            partition_def = get_dynamic_partitions_def(partition_name)
            if partition_def is None:
                context.log.warning(f"Dynamic partition {partition_name} definition is None, skipping")
                continue

            if not config.initial_partition_keys:
                context.log.warning(f"Dynamic partition {partition_name} has no initial_partition_keys, skipping")
                continue

            # Get current partition keys from instance
            try:
                current_keys = set(context.instance.get_dynamic_partition_keys(partition_name))
                context.log.info(f"Dynamic partition {partition_name}: current_keys={len(current_keys)}, desired_keys={len(config.initial_partition_keys)}")
            except Exception as e:
                context.log.warning(f"Failed to get current keys for {partition_name}: {e}")
                current_keys = set()

            desired_keys = set(config.initial_partition_keys)

            # Add keys that are in config but not in instance
            keys_to_add = desired_keys - current_keys
            if keys_to_add:
                context.log.info(f"Adding {len(keys_to_add)} keys to {partition_name}: {sorted(keys_to_add)}")
                add_request = partition_def.build_add_request(sorted(keys_to_add))
                dynamic_partitions_requests.append(add_request)

            # Remove keys that are in instance but not in config
            keys_to_remove = current_keys - desired_keys
            if keys_to_remove:
                context.log.info(f"Removing {len(keys_to_remove)} keys from {partition_name}: {sorted(keys_to_remove)}")
                delete_request = partition_def.build_delete_request(sorted(keys_to_remove))
                dynamic_partitions_requests.append(delete_request)

        if not dynamic_partitions_requests:
            context.log.info("No dynamic partition changes needed")
            return dg.SensorResult()

        context.log.info(f"Returning {len(dynamic_partitions_requests)} dynamic partition requests")
        return dg.SensorResult(
            dynamic_partitions_requests=dynamic_partitions_requests,
        )

    return _sensor

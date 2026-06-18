from __future__ import annotations

import os

from dagster import DailyPartitionsDefinition, PartitionsDefinition

_daily_partitions_def = None


def get_daily_partitions_def() -> DailyPartitionsDefinition:
    global _daily_partitions_def
    if _daily_partitions_def is not None:
        return _daily_partitions_def
    start_date = os.getenv("DAGSTER_DAILY_PARTITIONS_START_DATE")
    if not start_date:
        raise ValueError(
            "DAGSTER_DAILY_PARTITIONS_START_DATE must be set (YYYY-MM-DD) when using daily partitions"
        )
    _daily_partitions_def = DailyPartitionsDefinition(start_date=start_date)
    return _daily_partitions_def


def get_partitions_def(
    partition_spec: str | None,
    dynamic_partitions_defs: dict[str, PartitionsDefinition] | None = None,
) -> PartitionsDefinition | None:
    """Resolve partition specification to PartitionsDefinition.
    
    Handles:
    - "daily" → DailyPartitionsDefinition
    - "dynamic:name" → DynamicPartitionsDefinition from cache
    - None/"unpartitioned"/"" → None
    
    Args:
        partition_spec: Partition specification string
        dynamic_partitions_defs: Dict mapping dynamic partition names to definitions
    
    Returns:
        PartitionsDefinition or None
    
    Raises:
        ValueError: If partition_spec is invalid or dynamic partition not found
    """
    if partition_spec is None or partition_spec in {"none", "unpartitioned", ""}:
        return None
    
    if partition_spec == "daily":
        return get_daily_partitions_def()
    
    if partition_spec.startswith("dynamic:"):
        partition_name = partition_spec.split(":", 1)[1]
        if dynamic_partitions_defs is None:
            raise ValueError(f"Dynamic partition '{partition_name}' cannot be resolved: no dynamic partition definitions provided")
        if partition_name not in dynamic_partitions_defs:
            raise ValueError(f"Unknown dynamic partition: {partition_name}")
        return dynamic_partitions_defs[partition_name]
    
    raise ValueError(f"Unsupported partition spec: {partition_spec}")
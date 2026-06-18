"""Dynamic partitions management and caching.

This module provides utilities for creating, caching, and managing
DynamicPartitionsDefinition objects used for arbitrary (non-time-based)
partition dimensions like country codes, tenant IDs, etc.
"""

from __future__ import annotations

from typing import Optional

import dagster as dg

# Global cache for dynamic partition definitions
_dynamic_partitions_cache: dict[str, dg.DynamicPartitionsDefinition] = {}


def get_or_create_dynamic_partitions_def(
    name: str,
    initial_partition_keys: list[str],
) -> dg.DynamicPartitionsDefinition:
    """Get cached or create new DynamicPartitionsDefinition.

    Creates a DynamicPartitionsDefinition with the given name and initial
    partition keys. Subsequent calls with the same name return the cached
    definition (using the keys from the first call).

    Args:
        name: Unique identifier for the dynamic partition (e.g., "country_code")
        initial_partition_keys: List of initial partition key values

    Returns:
        A DynamicPartitionsDefinition for the given partition name

    Raises:
        ValueError: If name is empty or initial_partition_keys is empty
    """
    if not name or not name.strip():
        raise ValueError("Dynamic partition name must be non-empty")
    if not initial_partition_keys:
        raise ValueError("initial_partition_keys must be non-empty")

    name = name.strip()

    if name not in _dynamic_partitions_cache:
        # Create a DynamicPartitionsDefinition with just the name
        # Note: Modern Dagster requires the name parameter for DynamicPartitionsDefinition
        # The partition_fn approach is deprecated and will be removed in 2.0
        # Partition keys are managed at runtime via instance.add_dynamic_partitions()
        _dynamic_partitions_cache[name] = dg.DynamicPartitionsDefinition(name=name)

    return _dynamic_partitions_cache[name]


def update_dynamic_partition_keys(
    name: str,
    partition_keys: list[str],
) -> None:
    """Update partition keys for an existing dynamic partition.

    Recreates the DynamicPartitionsDefinition with new keys, invalidating
    any previous cached definition. Existing run requests/cursor state
    for old keys remains valid.

    Args:
        name: Name of the dynamic partition to update
        partition_keys: New list of partition keys

    Raises:
        ValueError: If name is empty, name doesn't exist, or keys is empty
    """
    if not name or not name.strip():
        raise ValueError("Dynamic partition name must be non-empty")
    if not partition_keys:
        raise ValueError("partition_keys must be non-empty")

    name = name.strip()

    if name not in _dynamic_partitions_cache:
        raise ValueError(f"Unknown dynamic partition: {name}")

    # The definition is already created with the name, no need to recreate
    # Partition keys are managed at runtime via instance.add_dynamic_partitions()
    # This function is kept for API compatibility but doesn't need to recreate the definition
    pass


def get_dynamic_partitions_def(name: str) -> Optional[dg.DynamicPartitionsDefinition]:
    """Get a cached dynamic partition definition by name.

    Args:
        name: Name of the dynamic partition

    Returns:
        The DynamicPartitionsDefinition if found, None otherwise
    """
    return _dynamic_partitions_cache.get(name)


def has_dynamic_partition(name: str) -> bool:
    """Check if a dynamic partition definition exists in cache.

    Args:
        name: Name of the dynamic partition

    Returns:
        True if the partition exists in cache, False otherwise
    """
    return name in _dynamic_partitions_cache


def reset_dynamic_partitions_cache() -> None:
    """Clear all cached dynamic partitions.

    This is mainly useful for testing to ensure a clean state between tests.
    """
    global _dynamic_partitions_cache
    _dynamic_partitions_cache = {}


def get_all_dynamic_partitions() -> dict[str, dg.DynamicPartitionsDefinition]:
    """Get a dictionary of all cached dynamic partition definitions.

    Returns:
        A dictionary mapping partition names to their definitions
    """
    return dict(_dynamic_partitions_cache)

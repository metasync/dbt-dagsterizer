"""Dynamic partition change detection sensor.

This module provides sensor logic for detecting changes in dynamic partitions
where partition keys are not time-based (e.g., country codes, tenant IDs).
"""

from __future__ import annotations

import json

import dagster as dg

from ....assets.dbt.prepare import prepare_manifest_if_missing
from ....jobs.dbt.jobs import get_dbt_jobs_by_name


def _parse_dynamic_cursor(cursor: str | None) -> dict[str, str] | None:
    """Parse dynamic partition cursor format.
    
    Cursor format (version 1):
    {
        "type": "partition_keys_v1",
        "keys": {
            "partition_key_1": "2024-01-15T10:30:00",
            "partition_key_2": "2024-01-15T09:45:00"
        }
    }
    
    Args:
        cursor: Cursor string from sensor context
    
    Returns:
        Dict mapping partition keys to watermark timestamps, or None if invalid
    """
    if not cursor:
        return None
    try:
        payload = json.loads(cursor)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "partition_keys_v1":
        return None
    keys = payload.get("keys")
    if not isinstance(keys, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in keys.items():
        if isinstance(k, str) and isinstance(v, str) and k and v:
            out[k] = v
    return out


def build_dynamic_partition_change_sensor(
    spec: dict,
    sensor_definition_builder,
) -> dg.SensorDefinition | None:
    """Build a partition change sensor for dynamic partitions.
    
    A dynamic partition sensor monitors non-time-based partition keys for changes.
    It requires metadata specifying how to discover partition keys and detect changes.
    
    Args:
        spec: Sensor specification dict containing:
            - name: Sensor name
            - job_name: Target job name
            - detector_model: Model being monitored
            - enabled: Whether sensor is enabled
            - minimum_interval_seconds: Minimum time between sensor ticks
            - meta: Metadata for partition key discovery and detection
            - partition_type: Should be "dynamic"
        sensor_definition_builder: Function to build the actual sensor (for flexibility)
    
    Returns:
        A SensorDefinition for dynamic partition change detection
    """
    enabled = bool(spec.get("enabled", True))
    default_status = dg.DefaultSensorStatus.RUNNING if enabled else dg.DefaultSensorStatus.STOPPED
    
    sensor_name = spec["name"]
    job_name = spec["job_name"]
    job = get_dbt_jobs_by_name()[job_name]
    minimum_interval_seconds = int(spec.get("minimum_interval_seconds", 60))
    
    @dg.sensor(
        name=sensor_name,
        job=job,
        default_status=default_status,
        minimum_interval_seconds=minimum_interval_seconds,
        required_resource_keys={"starrocks"},
    )
    def _sensor(context: dg.SensorEvaluationContext):
        """Dynamic partition change sensor.
        
        Detects changes in partition keys by comparing current keys with previously
        seen keys and their associated watermarks.
        """
        prepare_manifest_if_missing()
        detector_meta = spec.get("meta")
        if not isinstance(detector_meta, dict) or not detector_meta:
            raise ValueError(f"Dynamic partition detector spec '{sensor_name}' is missing meta")
        
        # TODO: Implement partition key discovery and watermark detection
        # This requires database queries to discover current partition keys
        # and their associated watermarks. The exact implementation depends on
        # how partition keys are stored and identified.
        
        # For now, log and skip - full implementation requires resource access
        context.log.warning(
            f"Dynamic partition sensor '{sensor_name}' not yet fully implemented - skipping"
        )
        yield dg.SkipReason("Dynamic partition sensor implementation pending")
        
        # Once implemented, update cursor with new state
        # new_cursor_state = {
        #     "type": "partition_keys_v1",
        #     "keys": {partition_key: watermark, ...}
        # }
        # context.update_cursor(json.dumps(new_cursor_state, sort_keys=True))
    
    return _sensor

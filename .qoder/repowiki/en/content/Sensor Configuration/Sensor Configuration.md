# Sensor Configuration

<cite>
**Referenced Files in This Document**
- [sensors/__init__.py](file://src/dbt_dagsterizer/sensors/__init__.py)
- [partition_change/auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/detector/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/detector/sparse_lookback.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/sparse_lookback.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/propagator/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/presets.py)
- [api.py](file://src/dbt_dagsterizer/api.py)
- [test_partition_change_sensor_impact_range.py](file://tests/test_partition_change_sensor_impact_range.py)
- [test_partition_change_sensor_watermark_dedupe.py](file://tests/test_partition_change_sensor_watermark_dedupe.py)
- [test_partition_change_sensor_missing_relation.py](file://tests/test_partition_change_sensor_missing_relation.py)
- [otel/dagster.py](file://src/dbt_dagsterizer/otel/dagster.py)
</cite>

## Table of Contents
1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Core Components](#core-components)
4. [Architecture Overview](#architecture-overview)
5. [Detailed Component Analysis](#detailed-component-analysis)
6. [Dependency Analysis](#dependency-analysis)
7. [Performance Considerations](#performance-considerations)
8. [Troubleshooting Guide](#troubleshooting-guide)
9. [Conclusion](#conclusion)
10. [Appendices](#appendices)

## Introduction
This document explains sensor configuration in dbt-dagsterizer with a focus on partition change detection, upstream/downstream tracking, and watermark management. It covers sensor factory patterns, preset configurations, custom sensor creation, sparse lookback strategies, impact range calculation, propagation patterns, naming conventions, tags, metadata, configuration options, performance tuning, monitoring, and integration with external data sources.

## Project Structure
The sensor subsystem resides under src/dbt_dagsterizer/sensors and is composed of:
- Partition change detection: detector module with factory, presets, sparse lookback, and dbt manifest helpers
- Propagation sensors: propagator module with factory and presets
- Auto-configuration: automatic generation of sensors from project configuration
- Sensors registry: sensors/__init__.py aggregates sensors for inclusion in Definitions

```mermaid
graph TB
subgraph "Sensors Registry"
SInit["sensors/__init__.py<br/>build_dbt_partition_change_sensors()<br/>build_partition_propagation_sensors()"]
end
subgraph "Partition Change Detector"
DFactory["detector/factory.py<br/>build_dbt_partition_change_sensors()"]
DPreset["detector/presets.py<br/>daily_partition_change()"]
DLookback["detector/sparse_lookback.py<br/>parse_sparse_lookback_meta()"]
DManifest["detector/dbt_manifest.py<br/>manifest helpers"]
end
subgraph "Propagation"
PFactory["propagator/factory.py<br/>build_partition_propagation_sensors()"]
PPreset["propagator/presets.py<br/>propagator presets"]
end
subgraph "Auto Config"
AutoCfg["partition_change/auto_config.py<br/>derive specs from config"]
end
SInit --> DFactory
SInit --> PFactory
DFactory --> DPreset
DFactory --> DLookback
DFactory --> DManifest
AutoCfg --> DFactory
```

**Diagram sources**
- [sensors/__init__.py:39-73](file://src/dbt_dagsterizer/sensors/__init__.py#L39-L73)
- [partition_change/detector/factory.py:48-65](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L48-L65)
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/detector/sparse_lookback.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/sparse_lookback.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/propagator/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/presets.py)
- [partition_change/auto_config.py:65-96](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py#L65-L96)

**Section sources**
- [sensors/__init__.py:39-73](file://src/dbt_dagsterizer/sensors/__init__.py#L39-L73)
- [partition_change/auto_config.py:65-96](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py#L65-L96)

## Core Components
- Sensor factory: builds sensor definitions from structured specs, validates inputs, and attaches metadata
- Preset builders: provide standardized sensor spec shapes (e.g., daily partition change)
- Sparse lookback parser: extracts detector metadata (partition date expression, updated-at expression, detect relation/source, impact scope)
- Watermark cursor: persists per-partition timestamps to deduplicate runs and avoid redundant work
- Propagation sensors: downstream sensors that react to upstream partition changes
- Auto-config: derives sensor specs from project configuration and environment

Key responsibilities:
- Detect partition changes via max-watermark queries against upstream sources
- Expand impact range to downstream assets
- Emit RunRequest with partition_key and run_key
- Tag runs with detector model, detect relation, and partition watermark
- Persist cursor for idempotency and future evaluation

**Section sources**
- [partition_change/detector/factory.py:79-205](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L79-L205)
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/detector/sparse_lookback.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/sparse_lookback.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/propagator/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/presets.py)
- [partition_change/auto_config.py:65-96](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py#L65-L96)

## Architecture Overview
The sensor pipeline integrates dbt manifests, StarRocks resource access, and Dagster’s sensor runtime. It detects upstream partition changes, expands impact ranges, and emits RunRequests for downstream assets.

```mermaid
sequenceDiagram
participant DC as "Dagster Scheduler"
participant SF as "Sensor Factory<br/>detector/factory.py"
participant MF as "Manifest Loader<br/>dbt_manifest.py"
participant SL as "Sparse Lookback Parser<br/>sparse_lookback.py"
participant SRC as "StarRocks Resource"
participant WM as "Watermark Cursor"
participant PR as "RunRequest Emitter"
DC->>SF : "evaluate_tick()"
SF->>MF : "load_manifest()"
SF->>SL : "parse_sparse_lookback_meta(meta)"
SF->>SRC : "detect_partition_max_watermarks()"
SRC-->>SF : "changed partitions + watermarks"
SF->>SF : "expand_impacted_dates(impact_range)"
SF->>WM : "persist cursor {type, last_check, partitions}"
SF->>PR : "yield RunRequest(partition_key, run_key, tags)"
```

**Diagram sources**
- [partition_change/detector/factory.py:87-201](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L87-L201)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/detector/sparse_lookback.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/sparse_lookback.py)

## Detailed Component Analysis

### Partition Change Detection Pipeline
- Sensor decorator: defines sensor name, job binding, interval, and required resource keys
- Manifest preparation: ensures dbt manifest exists and loads it
- Window computation: anchor day minus offset, lookback window
- Legacy cursor support: handles old timestamp cursors and migrates to partition watermark format
- Watermark detection: queries upstream sources for max(updated_at) per partition date
- Impact expansion: applies configured impact range to downstream partitions
- RunRequest emission: constructs partition_key and run_key, sets tags for observability
- Cursor update: stores per-partition watermark timestamps

```mermaid
flowchart TD
Start(["Sensor Tick"]) --> LoadMF["Load dbt manifest"]
LoadMF --> ParseMeta["Parse sparse lookback meta"]
ParseMeta --> ComputeWindow["Compute lookback window"]
ComputeWindow --> ParseCursor["Parse watermark cursor"]
ParseCursor --> DetectWM["Detect partition max watermarks"]
DetectWM --> ExpandImpact["Expand impacted dates by impact range"]
ExpandImpact --> FilterWindow{"Within window?"}
FilterWindow --> |No| Skip["Skip partition"]
FilterWindow --> |Yes| Emit["Emit RunRequest<br/>partition_key/run_key/tags"]
Emit --> UpdateCursor["Update cursor with new watermarks"]
Skip --> UpdateCursor
UpdateCursor --> End(["Done"])
```

**Diagram sources**
- [partition_change/detector/factory.py:95-201](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L95-L201)

**Section sources**
- [partition_change/detector/factory.py:79-205](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L79-L205)

### Sensor Factory Patterns and Presets
- daily_partition_change preset enforces non-empty names/jobs/models and positive intervals
- build_dbt_partition_change_sensors validates uniqueness of sensor names and constructs sensor definitions
- Propagator sensors are built similarly and integrated via sensors/__init__.py

```mermaid
classDiagram
class DailyPreset {
+daily_partition_change(name, job_name, detector_model, lookback_days, offset_days, enabled, minimum_interval_seconds, meta)
}
class SensorFactory {
+build_dbt_partition_change_sensors(specs) list
+build_partition_propagation_sensors(specs) list
}
DailyPreset --> SensorFactory : "produces specs"
```

**Diagram sources**
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/detector/factory.py:48-65](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L48-L65)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

**Section sources**
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/detector/factory.py:48-65](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L48-L65)

### Sparse Lookback Strategies and Metadata
- parse_sparse_lookback_meta reads detector meta to configure:
  - partition_date_expr: column used to derive partition date
  - updated_at_expr: column used to compute max watermark
  - detect_relation/detect_source: upstream relation to monitor
  - impact: downstream propagation scope
- These fields guide manifest parsing and impact range expansion

**Section sources**
- [partition_change/detector/sparse_lookback.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/sparse_lookback.py)
- [partition_change/detector/factory.py:93-93](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L93-L93)

### Watermark Management and Cursor Formats
- Legacy cursor: numeric timestamp fallback
- Current cursor: JSON payload with type, last_check, and partitions map
- Deduplication: if current watermark equals stored watermark for a partition, skip emitting RunRequest
- Tests demonstrate watermark updates and deduplication behavior

```mermaid
sequenceDiagram
participant Ctx as "Sensor Evaluation Context"
participant Cur as "Cursor Parser"
participant WM as "Watermark Detector"
participant Out as "RunRequest Emitter"
Ctx->>Cur : "parse watermark cursor"
Cur-->>Ctx : "prior_by_partition or legacy ts"
Ctx->>WM : "detect_partition_max_watermarks()"
WM-->>Ctx : "changed_partitions"
Ctx->>Out : "emit RunRequest if watermark increased"
Ctx->>Ctx : "update_cursor with new partitions"
```

**Diagram sources**
- [partition_change/detector/factory.py:100-201](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L100-L201)
- [test_partition_change_sensor_watermark_dedupe.py:81-117](file://tests/test_partition_change_sensor_watermark_dedupe.py#L81-L117)

**Section sources**
- [partition_change/detector/factory.py:100-201](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L100-L201)
- [test_partition_change_sensor_watermark_dedupe.py:81-117](file://tests/test_partition_change_sensor_watermark_dedupe.py#L81-L117)

### Impact Range Calculation and Propagation Patterns
- expand_impacted_dates applies configured impact range to base changed dates
- Tests validate that only affected partitions are emitted within the lookback window
- Propagator sensors react to upstream partition changes and schedule downstream assets

```mermaid
flowchart TD
Base["Base changed dates"] --> Expand["expand_impacted_dates(impact_range)"]
Expand --> Filter["Filter by lookback window"]
Filter --> Emit["Emit RunRequest per impacted partition"]
```

**Diagram sources**
- [partition_change/detector/factory.py:166-178](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L166-L178)
- [test_partition_change_sensor_impact_range.py](file://tests/test_partition_change_sensor_impact_range.py)

**Section sources**
- [partition_change/detector/factory.py:166-178](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L166-L178)
- [test_partition_change_sensor_impact_range.py](file://tests/test_partition_change_sensor_impact_range.py)

### Sensor Naming Conventions, Tags, and Metadata
- Naming: defaults to "{model}_partition_change_sensor" when not explicitly set
- Tags: include detector model, detect relation, and partition watermark
- Metadata: passed through meta field to influence detection behavior

**Section sources**
- [partition_change/auto_config.py:71-71](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py#L71-L71)
- [partition_change/detector/factory.py:185-189](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L185-L189)

### Sensor Configuration Options and Auto-Config
- lookback_days: number of days to scan for changes
- offset_days: delay from anchor day to shift the window
- minimum_interval_seconds: throttle between evaluations
- name/job_name/detector_model: identity and binding
- meta: detector configuration (partition_date_expr, updated_at_expr, detect_relation/detect_source, impact)

Auto-config derives specs from project configuration and environment variables.

**Section sources**
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/auto_config.py:68-96](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py#L68-L96)

### Integration with External Data Sources and Observable Triggers
- Required resource: starrocks resource key enables watermark detection
- Unknown relations/sources cause skip messages with informative errors
- Observability: OpenTelemetry instrumentation records sensor transactions and tags

**Section sources**
- [partition_change/detector/factory.py:85-85](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L85-L85)
- [test_partition_change_sensor_missing_relation.py:46-65](file://tests/test_partition_change_sensor_missing_relation.py#L46-L65)
- [otel/dagster.py:93-136](file://src/dbt_dagsterizer/otel/dagster.py#L93-L136)

### Custom Sensor Creation Examples
- Build sensor specs using daily_partition_change preset
- Construct sensors via build_dbt_partition_change_sensors
- Optionally integrate propagation sensors via build_partition_propagation_sensors
- Register sensors in Definitions alongside jobs

**Section sources**
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/detector/factory.py:48-65](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L48-L65)
- [sensors/__init__.py:39-73](file://src/dbt_dagsterizer/sensors/__init__.py#L39-L73)

## Dependency Analysis
The sensors registry composes detector and propagator factories, which depend on presets, sparse lookback parsing, and dbt manifest helpers. Auto-config feeds specs into the detector factory.

```mermaid
graph LR
API["api.py<br/>Definitions(sensors=get_sensors())"] --> Reg["sensors/__init__.py<br/>get_sensors()"]
Reg --> DF["detector/factory.py"]
Reg --> PF["propagator/factory.py"]
DF --> DP["detector/presets.py"]
DF --> SL["sparse_lookback.py"]
DF --> DM["dbt_manifest.py"]
AC["auto_config.py"] --> DF
```

**Diagram sources**
- [api.py:54-68](file://src/dbt_dagsterizer/api.py#L54-L68)
- [sensors/__init__.py:39-73](file://src/dbt_dagsterizer/sensors/__init__.py#L39-L73)
- [partition_change/detector/factory.py:48-65](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L48-L65)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/detector/presets.py:1-35](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py#L1-L35)
- [partition_change/detector/sparse_lookback.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/sparse_lookback.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/auto_config.py:65-96](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py#L65-L96)

**Section sources**
- [api.py:54-68](file://src/dbt_dagsterizer/api.py#L54-L68)
- [sensors/__init__.py:39-73](file://src/dbt_dagsterizer/sensors/__init__.py#L39-L73)

## Performance Considerations
- minimum_interval_seconds: tune to balance sensitivity vs. overhead
- lookback_days and offset_days: adjust window size to reduce unnecessary scans
- Watermark deduplication: prevents redundant runs; ensure updated_at_expr reflects true upstream freshness
- Impact range: keep impact minimal to limit downstream fan-out
- Resource contention: ensure starrocks resource availability and connection pooling

[No sources needed since this section provides general guidance]

## Troubleshooting Guide
Common issues and resolutions:
- Unknown database/relation errors: verify detect_relation/detect_source and manifest alignment
- No RunRequests emitted: check lookback window, watermark thresholds, and impact range
- Cursor anomalies: inspect legacy vs. partition watermark formats; ensure proper migration
- Sensor disabled or missing: confirm auto-config and environment variable LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE

**Section sources**
- [test_partition_change_sensor_missing_relation.py:46-65](file://tests/test_partition_change_sensor_missing_relation.py#L46-L65)
- [test_partition_change_sensor_watermark_dedupe.py:81-117](file://tests/test_partition_change_sensor_watermark_dedupe.py#L81-L117)

## Conclusion
The sensor subsystem provides robust, configurable partition change detection with sparse lookback, watermark management, and propagation. By leveraging presets, auto-config, and observability hooks, teams can efficiently orchestrate incremental asset refreshes while maintaining performance and reliability.

[No sources needed since this section summarizes without analyzing specific files]

## Appendices

### Appendix A: Sensor Registration and API Integration
- Sensors are aggregated in sensors/__init__.py and included in Definitions via api.py
- AutomationConditionSensorDefinition is registered by default

**Section sources**
- [sensors/__init__.py:39-73](file://src/dbt_dagsterizer/sensors/__init__.py#L39-L73)
- [api.py:54-68](file://src/dbt_dagsterizer/api.py#L54-L68)
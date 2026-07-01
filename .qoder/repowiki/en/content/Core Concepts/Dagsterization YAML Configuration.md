# Dagsterization YAML Configuration

<cite>
**Referenced Files in This Document**
- [dagsterization.yml](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml)
- [dagsterization-yml.md](file://docs/concepts/dagsterization-yml.md)
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [meta.py](file://src/dbt_dagsterizer/cli_parts/meta.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [partitions.py](file://src/dbt_dagsterizer/partitions.py)
</cite>

## Update Summary
**Changes Made**
- Added documentation for the new `include_current_day_partition` option in the `partitions.daily_config` section
- Updated the Daily Partition Configuration section with detailed explanation of the new feature
- Enhanced the CLI documentation to include the new `--include-current-day-partition` flag
- Added comprehensive examples demonstrating both `true` and `false` values for the new option
- Updated validation documentation to reflect the new configuration parameter

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

## Introduction
This document provides comprehensive documentation for the Dagsterization YAML Configuration system used by dbt-dagsterizer. The `dagsterization.yml` file serves as the single source of truth for Dagster orchestration intent in dbt projects, bridging dbt metadata with Dagster orchestration through partitioning strategies, job definitions, schedules, and partition change sensors.

The configuration system enables declarative orchestration of dbt models in Dagster, supporting both time-based daily partitions and flexible dynamic partitions for non-temporal dimensions like country codes or tenant IDs.

**Updated** Added support for the new `include_current_day_partition` option in the `partitions.daily_config` section, allowing users to control whether today's partition is available in the DailyPartitionsDefinition.

## Project Structure
The Dagsterization YAML configuration system is organized around several key components:

```mermaid
graph TB
subgraph "Configuration Layer"
YML[dagsterization.yml]
ORCH[orchestration_config.py]
VALID[validation.py]
END_OFFSET[include_current_day_partition]
end
subgraph "Runtime Layer"
JOBS[jobs/dbt/factory.py]
SCHEDULES[schedules/dbt/factory.py]
DETECTORS[sensors/partition_change/detector/factory.py]
PROPAGATORS[sensors/partition_change/propagator/factory.py]
BOOTSTRAP[sensors/dynamic_partitions_bootstrap.py]
end
subgraph "Partition Management"
DYNAMIC[partitions_dynamic.py]
DAILY[partitions.py]
INDEX[OrchestrationIndex]
end
YML --> ORCH
ORCH --> VALID
ORCH --> END_OFFSET
ORCH --> JOBS
ORCH --> SCHEDULES
ORCH --> DETECTORS
ORCH --> PROPAGATORS
ORCH --> BOOTSTRAP
ORCH --> DYNAMIC
ORCH --> DAILY
ORCH --> INDEX
```

**Diagram sources**
- [dagsterization.yml:1-50](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml#L1-L50)
- [orchestration_config.py:120-191](file://src/dbt_dagsterizer/orchestration_config.py#L120-L191)
- [validation.py:22-212](file://src/dbt_dagsterizer/cli_parts/validation.py#L22-L212)
- [partitions.py:10-30](file://src/dbt_dagsterizer/partitions.py#L10-L30)

**Section sources**
- [dagsterization.yml:1-50](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml#L1-L50)
- [dagsterization-yml.md:1-707](file://docs/concepts/dagsterization-yml.md#L1-L707)

## Core Components

### Configuration File Structure
The `dagsterization.yml` file follows a hierarchical structure with six primary sections:

```mermaid
flowchart TD
ROOT[dagsterization.yml Root] --> VERSION[version: 1]
VERSION --> TIMEZONE[timezone: string]
VERSION --> PARTITIONS[partitions Section]
VERSION --> JOBS[jobs Section]
VERSION --> ASSET_JOBS[asset_jobs Section]
VERSION --> SCHEDULES[schedules Section]
VERSION --> PARTITION_CHANGE[partition_change Section]
PARTITIONS --> DAILY[daily: model lists]
PARTITIONS --> DAILY_CONFIG[daily_config: {include_current_day_partition}]
PARTITIONS --> DYNAMIC[dynamic: partition definitions]
JOBS --> JOB1[job_name: {models, include_upstream, partitions}]
ASSET_JOBS --> ASSET_LIST[Model names as strings]
SCHEDULES --> SCHEDULE1[schedule_name: {type, job_name, hour, minute, lookback_days, offset_days, enabled}]
PARTITION_CHANGE --> DETECTORS1[detectors: sensor configurations]
PARTITION_CHANGE --> PROPAGATORS1[propagators: downstream triggers]
```

**Diagram sources**
- [dagsterization.yml:1-50](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml#L1-L50)
- [dagsterization-yml.md:27-56](file://docs/concepts/dagsterization-yml.md#L27-L56)

### Partition Types and Constraints
The system supports three primary partition types with strict isolation requirements:

| Partition Type | Description | Environment Variables | Asset Group Isolation |
|---|---|---|---|
| `daily` | One partition per day | `DAGSTER_DAILY_PARTITIONS_START_DATE`, `DAGSTER_PARTITION_TIMEZONE` | ✅ Separate group |
| `dynamic` | Custom partition keys (e.g., country codes) | N/A (defined inline) | ✅ Separate group (per name) |
| `unpartitioned` | No partitioning | N/A | ✅ Separate group |

**Section sources**
- [dagsterization-yml.md:102-130](file://docs/concepts/dagsterization-yml.md#L102-L130)

## Architecture Overview

The Dagsterization YAML Configuration system implements a multi-layered architecture that transforms declarative configuration into executable Dagster orchestration:

```mermaid
sequenceDiagram
participant User as User
participant YAML as dagsterization.yml
participant Loader as orchestration_config.py
participant Validator as validation.py
participant EndOffset as include_current_day_partition
participant Factory as job_factory.py
participant Runtime as Dagster Runtime
User->>YAML : Edit configuration
YAML->>Loader : Load YAML file
Loader->>Validator : Validate structure
Validator->>Validator : Check partition types
Validator->>EndOffset : Validate daily_config
EndOffset->>EndOffset : Parse boolean flag
Validator->>Validator : Validate job references
Validator->>Factory : Generate specs
Factory->>Runtime : Create jobs/schedules/sensors
Runtime->>Runtime : Execute orchestration
```

**Diagram sources**
- [orchestration_config.py:30-75](file://src/dbt_dagsterizer/orchestration_config.py#L30-L75)
- [validation.py:22-212](file://src/dbt_dagsterizer/cli_parts/validation.py#L22-L212)
- [partitions.py:10-30](file://src/dbt_dagsterizer/partitions.py#L10-L30)
- [factory.py:84-127](file://src/dbt_dagsterizer/jobs/dbt/factory.py#L84-L127)

## Detailed Component Analysis

### Orchestration Configuration Loading
The configuration loading system provides robust YAML parsing with default value handling:

```mermaid
classDiagram
class OrchestrationIndex {
+dict~str,str~ partitions_by_model
+dict~str,DynamicPartitionConfig~ dynamic_partitions
+set~str~ asset_job_models
+dict~str,str~ group_job_by_model
+bool daily_include_current_day_partition
+string timezone
}
class DynamicPartitionConfig {
+string name
+string[] initial_partition_keys
}
class OrchestrationConfig {
+load_or_create(path) MutableMapping
+index(data) OrchestrationIndex
+set_partition(data, model, partition)
+set_group_job(data, job_name, models, include_upstream, partitions)
+set_daily_config(data, include_current_day_partition)
}
OrchestrationConfig --> OrchestrationIndex : creates
OrchestrationIndex --> DynamicPartitionConfig : contains
```

**Diagram sources**
- [orchestration_config.py:112-191](file://src/dbt_dagsterizer/orchestration_config.py#L112-L191)
- [orchestration_config.py:1-16](file://src/dbt_dagsterizer/orchestration_config.py#L1-L16)

### Validation System
The validation system enforces configuration integrity through comprehensive checks:

```mermaid
flowchart TD
START[Configuration Load] --> STRUCT[Structure Validation]
STRUCT --> PARTITION[Partition Validation]
PARTITION --> DAILY_CONFIG[Daily Config Validation]
DAILY_CONFIG --> BOOLEAN_CHECK[Check Boolean Type]
BOOLEAN_CHECK --> JOB[Job Validation]
JOB --> SCHEDULE[Schedule Validation]
SCHEDULE --> SENSOR[Sensor Validation]
SENSOR --> COMPLETE[Validation Complete]
PARTITION --> |Invalid| ERROR1[Error: Invalid partition type]
DAILY_CONFIG --> |Invalid| ERROR2[Error: Invalid daily_config type]
JOB --> |Invalid| ERROR3[Error: Missing models]
SCHEDULE --> |Invalid| ERROR4[Error: Invalid schedule config]
SENSOR --> |Invalid| ERROR5[Error: Missing relations]
```

**Diagram sources**
- [validation.py:22-212](file://src/dbt_dagsterizer/cli_parts/validation.py#L22-L212)
- [validation.py:215-320](file://src/dbt_dagsterizer/cli_parts/validation.py#L215-L320)

**Section sources**
- [validation.py:22-212](file://src/dbt_dagsterizer/cli_parts/validation.py#L22-L212)
- [validation.py:215-320](file://src/dbt_dagsterizer/cli_parts/validation.py#L215-L320)

### Daily Partition Configuration
The new `include_current_day_partition` option provides granular control over daily partition availability:

```mermaid
flowchart TD
CONFIG[partitions.daily_config] --> INCLUDE_CURRENT[include_current_day_partition: boolean]
INCLUDE_CURRENT --> DEFAULT[Default: false]
INCLUDE_CURRENT --> TRUE_VALUE[Value: true]
INCLUDE_CURRENT --> FALSE_VALUE[Value: false]
TRUE_VALUE --> END_OFFSET_1[end_offset: 1]
FALSE_VALUE --> END_OFFSET_0[end_offset: 0]
DEFAULT --> END_OFFSET_0
END_OFFSET_1 --> DAGSTER[Dagster DailyPartitionsDefinition]
END_OFFSET_0 --> DAGSTER
DAGSTER --> AVAILABLE[Available Partitions]
```

**Diagram sources**
- [dagsterization-yml.md:126-149](file://docs/concepts/dagsterization-yml.md#L126-L149)
- [partitions.py:10-30](file://src/dbt_dagsterizer/partitions.py#L10-L30)

**Section sources**
- [dagsterization-yml.md:126-149](file://docs/concepts/dagsterization-yml.md#L126-L149)
- [partitions.py:10-30](file://src/dbt_dagsterizer/partitions.py#L10-L30)

### Job Factory Implementation
The job factory transforms configuration into executable Dagster jobs:

```mermaid
sequenceDiagram
participant Config as Configuration
participant Factory as Job Factory
participant Partitions as Partitions Module
participant EndOffset as include_current_day_partition
participant Dagster as Dagster Engine
Config->>Factory : Job specifications
Factory->>Partitions : Resolve partition definitions
Partitions->>EndOffset : Get include_current_day_partition
EndOffset->>Dagster : Create partitions definition
Factory->>Dagster : Build asset jobs
Factory->>Dagster : Configure job tags
Dagster->>Dagster : Register jobs
```

**Diagram sources**
- [factory.py:84-127](file://src/dbt_dagsterizer/jobs/dbt/factory.py#L84-L127)
- [factory.py:12-28](file://src/dbt_dagsterizer/jobs/dbt/factory.py#L12-L28)
- [partitions.py:33-70](file://src/dbt_dagsterizer/partitions.py#L33-L70)

**Section sources**
- [factory.py:84-127](file://src/dbt_dagsterizer/jobs/dbt/factory.py#L84-L127)

### Dynamic Partitions Management
Dynamic partitions provide flexible non-temporal partitioning capabilities:

```mermaid
flowchart TD
CONFIG[Configuration] --> CACHE[Dynamic Partitions Cache]
CACHE --> BOOTSTRAP[Bootstrap Sensor]
BOOTSTRAP --> INSTANCE[Instance Sync]
INSTANCE --> RUNTIME[Runtime Updates]
CONFIG --> FACTORY[Partition Factory]
FACTORY --> DEFINITION[Dynamic Definition]
DEFINITION --> CACHE
BOOTSTRAP --> |Initial Keys| INSTANCE
RUNTIME --> |Add Keys| INSTANCE
RUNTIME --> |Remove Keys| INSTANCE
```

**Diagram sources**
- [dynamic_partitions_bootstrap.py:39-122](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py#L39-L122)
- [partitions_dynamic.py:18-52](file://src/dbt_dagsterizer/partitions_dynamic.py#L18-L52)

**Section sources**
- [dynamic_partitions_bootstrap.py:39-122](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py#L39-L122)
- [partitions_dynamic.py:18-52](file://src/dbt_dagsterizer/partitions_dynamic.py#L18-L52)

### Partition Change Sensors
The system implements sophisticated sensors for handling late arrivals and data updates:

```mermaid
flowchart TD
DETECTOR[Detector Sensor] --> WATERMARK[Watermark Detection]
WATERMARK --> COMPARE[Compare with Cursor]
COMPARE --> CHANGED{Changed?}
CHANGED --> |Yes| EMIT[Emit Run Requests]
CHANGED --> |No| SKIP[Skip Evaluation]
EMIT --> IMPACT[Impact Range Calculation]
IMPACT --> TARGETS[Trigger Targets]
PROPAGATOR[Propagator Sensor] --> EVENTS[Materialization Events]
EVENTS --> EXTRACT[Extract Partition Key]
EXTRACT --> TRIGGER[Trigger Downstream Jobs]
```

**Diagram sources**
- [factory.py:85-195](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L85-L195)
- [factory.py:42-142](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py#L42-L142)

**Section sources**
- [factory.py:85-195](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py#L85-L195)
- [factory.py:42-142](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py#L42-L142)

## Dependency Analysis

The configuration system exhibits clear separation of concerns with well-defined dependencies:

```mermaid
graph TB
subgraph "Configuration Dependencies"
DAGSTERIZATION[dagsterization.yml] --> ORCHESTRATION_CONFIG[orchestration_config.py]
ORCHESTRATION_CONFIG --> VALIDATION[validation.py]
ORCHESTRATION_CONFIG --> PARTITIONS[partitions.py]
END_OFFSET[include_current_day_partition] --> PARTITIONS
end
subgraph "Runtime Dependencies"
VALIDATION --> JOBS_FACTORY[jobs/dbt/factory.py]
VALIDATION --> SCHEDULES_FACTORY[schedules/dbt/factory.py]
VALIDATION --> DETECTORS_FACTORY[sensors/partition_change/detector/factory.py]
VALIDATION --> PROPAGATORS_FACTORY[sensors/partition_change/propagator/factory.py]
VALIDATION --> BOOTSTRAP_FACTORY[sensors/dynamic_partitions_bootstrap.py]
end
subgraph "Partition Dependencies"
ORCHESTRATION_CONFIG --> PARTITIONS_DYNAMIC[partitions_dynamic.py]
DETECTORS_FACTORY --> PARTITIONS_DYNAMIC
BOOTSTRAP_FACTORY --> PARTITIONS_DYNAMIC
PARTITIONS --> DAILY_PARTITIONS_DEF[DailyPartitionsDefinition]
DAILY_PARTITIONS_DEF --> END_OFFSET
END_OFFSET --> DAGSTER_RUNTIME[Dagster Runtime]
end
```

**Diagram sources**
- [orchestration_config.py:1-91](file://src/dbt_dagsterizer/orchestration_config.py#L1-L91)
- [validation.py:1-200](file://src/dbt_dagsterizer/cli_parts/validation.py#L1-L200)
- [partitions.py:10-30](file://src/dbt_dagsterizer/partitions.py#L10-L30)

**Section sources**
- [orchestration_config.py:1-91](file://src/dbt_dagsterizer/orchestration_config.py#L1-L91)
- [validation.py:1-200](file://src/dbt_dagsterizer/cli_parts/validation.py#L1-L200)

## Performance Considerations
The configuration system is designed for optimal performance through several mechanisms:

- **Lazy Loading**: Dynamic partitions are loaded on-demand rather than at startup
- **Caching**: Partition definitions are cached to avoid repeated creation
- **Efficient Validation**: Validation occurs only when configuration changes
- **Minimal Memory Footprint**: Configuration is parsed once and reused across components
- **Boolean Flag Optimization**: The `include_current_day_partition` option is processed as a simple boolean flag without additional overhead

## Troubleshooting Guide

### Common Configuration Issues

**Partition Type Conflicts**
- **Symptom**: `DagsterInvariantViolationError: Cannot mix partition types`
- **Cause**: Mixing different partition types in a single job
- **Solution**: Ensure each job uses models with the same partition type

**Missing Model References**
- **Symptom**: Validation errors for missing models
- **Cause**: Models referenced in configuration don't exist in dbt manifest
- **Solution**: Verify model names match dbt project structure

**Dynamic Partition Configuration Errors**
- **Symptom**: Errors for invalid dynamic partition names
- **Cause**: Unknown dynamic partition references or empty initial keys
- **Solution**: Check dynamic partition definitions in configuration

**Daily Configuration Validation Errors**
- **Symptom**: Validation errors for `partitions.daily_config.include_current_day_partition`
- **Cause**: Non-boolean value or malformed daily_config structure
- **Solution**: Ensure `include_current_day_partition` is a boolean value and `daily_config` is a mapping

**Section sources**
- [dagsterization-yml.md:654-707](file://docs/concepts/dagsterization-yml.md#L654-L707)

## Conclusion
The Dagsterization YAML Configuration system provides a robust, declarative approach to orchestrating dbt models in Dagster. Through careful separation of concerns, comprehensive validation, and flexible partitioning strategies, it enables teams to manage complex orchestration requirements while maintaining simplicity and reliability.

**Updated** The addition of the `include_current_day_partition` option in the `partitions.daily_config` section enhances the system's flexibility by providing precise control over daily partition availability. This feature allows teams to configure whether today's partition should be available in the DailyPartitionsDefinition, enabling same-day processing scenarios when needed.

The system's modular architecture allows for easy extension and customization while preserving backward compatibility and providing clear error messages for troubleshooting. The new daily partition configuration feature maintains consistency with the existing configuration structure and validation patterns, ensuring seamless integration into the overall orchestration framework.
# Automatic Job Creation

<cite>
**Referenced Files in This Document**
- [jobs.py](file://src/dbt_dagsterizer/jobs/dbt/jobs.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [automation.py](file://src/dbt_dagsterizer/assets/sources/automation.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/detector/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/propagator/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/presets.py)
- [auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [dagsterization.yml](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml)
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
This document explains how dbt-dagsterizer automatically creates Dagster jobs from dbt model dependencies and manifest data. It covers the dependency graph analysis process, job composition algorithms, automatic scheduling inference, and the factory pattern used to generate jobs. It also documents configuration options for automatic behavior, naming conventions, and metadata assignment, with examples drawn from different dbt model types and dependency scenarios.

## Project Structure
The automatic job creation capability spans several modules:
- Jobs: automatic job generation and configuration for dbt assets
- Schedules: automatic schedule inference for dbt jobs and observable sources
- Sensors: partition change detection and propagation, informing job execution windows
- Assets: dbt asset translation and preparation for Dagster
- DBT Manifest: parsing and preparing dbt manifest data for job/schedule inference
- Templates: project-level configuration for dagsterization behavior

```mermaid
graph TB
subgraph "Jobs"
JF["factory.py"]
JA["auto_config.py"]
JP["presets.py"]
JC["dbt_config.py"]
JJ["jobs.py"]
end
subgraph "Schedules"
SC["auto_config.py"]
SF["factory.py"]
SS["schedules.py"]
end
subgraph "Sensors"
PCD["partition_change/detector/factory.py"]
PCDM["partition_change/detector/dbt_manifest.py"]
PCP["partition_change/detector/presets.py"]
PCPF["partition_change/propagator/factory.py"]
PCPP["partition_change/propagator/presets.py"]
PCA["auto_config.py"]
end
subgraph "Assets"
AD["assets.py"]
AT["translator.py"]
AP["prepare.py"]
end
subgraph "DBT Manifest"
DM["manifest.py"]
DMP["manifest_prepare.py"]
end
subgraph "Templates"
TPL["dagsterization.yml"]
end
DM --> JF
DMP --> JF
AT --> JF
AD --> JF
AP --> JF
JF --> JJ
JA --> JJ
JP --> JJ
JC --> JJ
SC --> SS
SF --> SS
SS --> JJ
PCD --> SS
PCDM --> SS
PCP --> SS
PCPF --> SS
PCPP --> SS
PCA --> SS
TPL --> JC
```

**Diagram sources**
- [jobs.py](file://src/dbt_dagsterizer/jobs/dbt/jobs.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/detector/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/propagator/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/presets.py)
- [auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [dagsterization.yml](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml)

**Section sources**
- [jobs.py](file://src/dbt_dagsterizer/jobs/dbt/jobs.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/detector/dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [partition_change/detector/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/presets.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partition_change/propagator/presets.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/presets.py)
- [auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [dagsterization.yml](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml)

## Core Components
- Job Factory: constructs Dagster jobs from dbt manifest data and asset definitions.
- Auto Config: computes default job and schedule configurations from dbt dependencies and project settings.
- Presets: provides named defaults for job modes, partitions, and execution policies.
- DBT Manifest Integration: parses and prepares dbt manifest artifacts for job/sensor inference.
- Assets Pipeline: translates dbt assets into Dagster assets and prepares them for job composition.
- Schedules: infers schedules for dbt jobs and observable sources based on automation settings.
- Sensors: detects partition changes and propagates impact ranges to refine job execution windows.

Key responsibilities:
- Dependency Graph Analysis: build topological ordering of dbt nodes to determine execution order.
- Asset-to-Job Mapping: map dbt assets to jobs while respecting upstream/downstream boundaries.
- Automatic Scheduling Inference: derive cron schedules and default statuses from dbt metadata and project presets.
- Metadata Assignment: propagate dbt model properties and tags into Dagster job and asset metadata.

**Section sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

## Architecture Overview
The automatic job creation pipeline integrates dbt manifest data, asset translation, and preset-driven configuration to produce Dagster jobs and schedules.

```mermaid
sequenceDiagram
participant DBT as "dbt Manifest"
participant MAN as "Manifest Prepare"
participant TR as "Translator"
participant AS as "Assets"
participant FAC as "Job Factory"
participant CFG as "Auto Config"
participant PRE as "Presets"
participant JOB as "Dagster Job"
DBT->>MAN : "Load and normalize manifest"
MAN->>TR : "Provide nodes and edges"
TR->>AS : "Translate assets and keys"
AS->>FAC : "Asset definitions and metadata"
FAC->>CFG : "Compute default config"
CFG->>PRE : "Apply presets"
PRE-->>FAC : "Execution mode, partitions, tags"
FAC-->>JOB : "Compose job with proper order"
```

**Diagram sources**
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)

## Detailed Component Analysis

### Job Factory Pattern
The job factory builds Dagster jobs from dbt assets and manifest data. It orchestrates:
- Topological sorting of dbt nodes to define execution order
- Asset-to-job mapping respecting upstream/downstream boundaries
- Metadata propagation from dbt to Dagster (tags, owners, descriptions)
- Partition and schedule configuration via presets and automation settings

```mermaid
classDiagram
class JobFactory {
+build_job(manifest, assets) Job
+map_assets_to_jobs(nodes, edges) dict
+apply_preset(job_config, preset) JobConfig
+compute_execution_order(nodes, edges) list
}
class AutoConfig {
+infer_job_config(dbt_node) JobConfig
+infer_schedule(node) ScheduleConfig
}
class Presets {
+get_preset(name) Preset
+merge_with_auto(base, auto) JobConfig
}
class ManifestPrepare {
+prepare(manifest) ManifestData
}
JobFactory --> AutoConfig : "uses"
JobFactory --> Presets : "uses"
JobFactory --> ManifestPrepare : "uses"
```

**Diagram sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)

**Section sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)

### Dependency Graph Analysis and Execution Order
The factory analyzes dbt manifest edges to construct a dependency graph and compute execution order:
- Build adjacency lists from manifest edges
- Perform topological sort to determine node execution order
- Group nodes into job units respecting asset boundaries and partition constraints
- Assign downstream tasks to run after upstream completion

```mermaid
flowchart TD
Start(["Start"]) --> LoadEdges["Load manifest edges"]
LoadEdges --> BuildAdj["Build adjacency lists"]
BuildAdj --> TopSort["Topological sort"]
TopSort --> GroupJobs["Group nodes into jobs"]
GroupJobs --> Partitions["Apply partition constraints"]
Partitions --> AssignOrder["Assign execution order"]
AssignOrder --> End(["End"])
```

**Diagram sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)

**Section sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)

### Asset-to-Job Mapping and Composition
The factory maps dbt assets to jobs by:
- Translating dbt relations to Dagster asset keys
- Using translator utilities to resolve naming and grouping
- Preparing assets with metadata and partition definitions
- Composing job solids in execution order and attaching schedules

```mermaid
sequenceDiagram
participant TR as "Translator"
participant AS as "Assets"
participant FAC as "Factory"
participant JOB as "Job"
TR->>AS : "Asset keys and metadata"
AS->>FAC : "Prepared assets"
FAC->>FAC : "Map assets to jobs"
FAC->>JOB : "Compose job with solids"
JOB-->>FAC : "Job ready"
```

**Diagram sources**
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)

**Section sources**
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)

### Automatic Scheduling Inference
Schedules are inferred from dbt automation settings and project presets:
- Observable sources schedule: checks automation flag and sets cron and default status
- Partition change sensors: detect partition updates and propagate impact ranges
- Sensor factories: build sensor definitions aligned with dbt manifest and presets

```mermaid
sequenceDiagram
participant AUT as "Automation Settings"
participant SCH as "Schedules"
participant SEN as "Partition Change Sensors"
participant CFG as "Auto Config"
AUT->>SCH : "Enable observe_sources?"
SCH->>CFG : "Set cron and default status"
SEN->>CFG : "Compute sensor presets"
CFG-->>SCH : "Schedule definition"
CFG-->>SEN : "Sensor definition"
```

**Diagram sources**
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

**Section sources**
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

### Configuration Options and Naming Conventions
Configuration is driven by:
- Project-level presets and automation flags
- DBT manifest metadata (model owners, tags, materializations)
- Template-defined defaults in dagsterization.yml
- Environment variables for schedule tuning

Examples of configurable aspects:
- Job naming conventions derived from dbt model names and groups
- Partition definitions mapped from dbt models to Dagster partitions
- Cron schedule presets applied per model type or group
- Default status for schedules controlled by automation flags

**Section sources**
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [dagsterization.yml](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/dagsterization.yml)

### Examples of Automatic Job Generation

#### Example 1: Incremental Model with Partitioned Upstream
- Scenario: An incremental dbt model depends on a partitioned upstream model.
- Behavior: The factory groups nodes into a single job respecting partition boundaries, assigns execution order via topological sort, and applies partition presets for downstream runs.

#### Example 2: Star Schema with Fact/Dimension Separation
- Scenario: A fact table depends on multiple dimension tables.
- Behavior: The factory composes a job that executes dimension tables first, followed by the fact table, ensuring data consistency.

#### Example 3: Observability Workflow
- Scenario: Observable sources are enabled via automation.
- Behavior: A schedule is created with a default status set to running and a cron interval configured from environment variables.

**Section sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

## Dependency Analysis
The automatic job creation system exhibits cohesive coupling around manifest parsing, asset translation, and preset-driven configuration.

```mermaid
graph TB
DM["manifest.py"] --> FAC["factory.py"]
DMP["manifest_prepare.py"] --> FAC
TR["translator.py"] --> FAC
AS["assets.py"] --> FAC
AP["prepare.py"] --> FAC
AJ["auto_config.py"] --> FAC
PJ["presets.py"] --> FAC
JC["dbt_config.py"] --> FAC
SS["schedules.py"] --> FAC
PCD["partition_change/detector/factory.py"] --> SS
PCPF["partition_change/propagator/factory.py"] --> SS
```

**Diagram sources**
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

**Section sources**
- [factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

## Performance Considerations
- Prefer topological sorting over repeated dependency resolution to minimize overhead.
- Cache prepared manifest data and asset translations to avoid recomputation across runs.
- Limit job granularity to reduce scheduler overhead while preserving data correctness.
- Use partition-aware scheduling to constrain execution windows and reduce redundant runs.

## Troubleshooting Guide
Common issues and resolutions:
- Missing automation flag for observable sources: ensure the automation flag is enabled so the observe_sources schedule is generated.
- Incorrect cron schedule: verify environment variables and presets align with desired frequency.
- Partition mismatch errors: confirm dbt model partition definitions match Dagster partition presets.
- Job not appearing: check asset translation and prepare steps to ensure assets are registered.

**Section sources**
- [schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [auto_config.py](file://src/dbt_dagsterizer/sensors/partition_change/auto_config.py)
- [partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

## Conclusion
dbt-dagsterizer’s automatic job creation leverages dbt manifest data, asset translation, and preset-driven configuration to compose efficient Dagster jobs and schedules. The factory pattern coordinates dependency graph analysis, asset-to-job mapping, and scheduling inference, while templates and automation flags tailor behavior to project needs.

## Appendices
- Additional references for project-level configuration and automation flags are available in the template dagsterization.yml and related automation modules.
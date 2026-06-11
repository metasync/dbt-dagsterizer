# Validation Commands

<cite>
**Referenced Files in This Document**
- [cli.py](file://src/dbt_dagsterizer/cli.py)
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [project.py](file://src/dbt_dagsterizer/cli_parts/project.py)
- [app.py](file://src/dbt_dagsterizer/cli_parts/app.py)
- [common.py](file://src/dbt_dagsterizer/cli_parts/common.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [dbt.py](file://src/dbt_dagsterizer/resources/dbt.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)
- [README.md](file://README.md)
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

## Introduction
This document describes validation-related CLI commands in dbt-dagsterizer. It focuses on commands that check dbt project integrity, manifest consistency, configuration correctness, asset dependencies, schedule configurations, sensor setups, environment readiness, dependency resolution, and compatibility verification. It also covers exit codes, error reporting, and remediation steps for failed validations.

## Project Structure
The CLI entrypoint composes subcommands from modular parts. Validation logic is centralized under a dedicated validation module and integrates with dbt manifest preparation and runtime helpers.

```mermaid
graph TB
CLI["CLI Entrypoint<br/>cli.py"] --> APP["App Builder<br/>cli_parts/app.py"]
CLI --> COMMON["Common Helpers<br/>cli_parts/common.py"]
CLI --> VALIDATION["Validation Commands<br/>cli_parts/validation.py"]
CLI --> PROJECT["Project Commands<br/>cli_parts/project.py"]
VALIDATION --> MANIFEST_PREP["Manifest Preparation<br/>dbt/manifest_prepare.py"]
VALIDATION --> MANIFEST["Manifest Loader<br/>dbt/manifest.py"]
VALIDATION --> RUN_RESULTS["Run Results Loader<br/>dbt/run_results.py"]
VALIDATION --> JOBS_CFG["Jobs Config<br/>jobs/dbt_config.py"]
VALIDATION --> SCHEDULE_PRESETS["Schedule Presets<br/>schedules/presets.py"]
VALIDATION --> SENSOR_DETECTOR["Sensor Detector<br/>sensors/.../detector/dbt_manifest.py"]
VALIDATION --> RES_DBT["DBT Resource<br/>resources/dbt.py"]
VALIDATION --> ENV["Environment Utils<br/>env_utils.py"]
```

**Diagram sources**
- [cli.py](file://src/dbt_dagsterizer/cli.py)
- [app.py](file://src/dbt_dagsterizer/cli_parts/app.py)
- [common.py](file://src/dbt_dagsterizer/cli_parts/common.py)
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [project.py](file://src/dbt_dagsterizer/cli_parts/project.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [dbt.py](file://src/dbt_dagsterizer/resources/dbt.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

**Section sources**
- [cli.py](file://src/dbt_dagsterizer/cli.py)
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [project.py](file://src/dbt_dagsterizer/cli_parts/project.py)

## Core Components
- Validation command group: Provides subcommands to validate dbt project integrity, manifest consistency, configuration correctness, asset dependencies, schedule configurations, sensor setups, environment readiness, dependency resolution, and compatibility.
- Manifest preparation and loading: Ensures dbt artifacts are ready and consistent before validation.
- Jobs and schedules configuration: Validates job and schedule presets against dbt project metadata.
- Sensor detection: Validates partition-change sensor setup against dbt manifest.
- Environment and resource helpers: Validates environment variables and DBT resource availability.

**Section sources**
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [dbt.py](file://src/dbt_dagsterizer/resources/dbt.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

## Architecture Overview
The validation commands are composed via the CLI entrypoint and delegate to specialized validators. Validators load and cross-check dbt artifacts, configuration presets, and environment settings.

```mermaid
sequenceDiagram
participant U as "User"
participant CLI as "CLI"
participant V as "Validation Command"
participant MP as "Manifest Prepare"
participant ML as "Manifest Loader"
participant RC as "Run Results Loader"
participant JC as "Jobs Config"
participant SC as "Schedule Presets"
participant SD as "Sensor Detector"
participant ER as "Env/Resource"
U->>CLI : Invoke validation subcommand
CLI->>V : Dispatch to validator
V->>MP : Prepare dbt artifacts
V->>ML : Load manifest
V->>RC : Load run results (optional)
V->>JC : Validate job config
V->>SC : Validate schedule presets
V->>SD : Validate sensor setup
V->>ER : Validate environment/resource
V-->>U : Report validation results
```

**Diagram sources**
- [cli.py](file://src/dbt_dagsterizer/cli.py)
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

## Detailed Component Analysis

### Validation Command Group
- Purpose: Centralized validation commands for dbt project integrity, manifest consistency, configuration correctness, asset dependencies, schedule configurations, sensor setups, environment readiness, dependency resolution, and compatibility.
- Composition: Built from the CLI entrypoint and routed to the validation module.

**Section sources**
- [cli.py](file://src/dbt_dagsterizer/cli.py)
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)

### Manifest Preparation and Loading
- Manifest preparation ensures dbt artifacts are generated and up-to-date before validation.
- Manifest loader reads and validates dbt manifest structure and contents.
- Run results loader optionally validates recent dbt runs for freshness and consistency.

```mermaid
flowchart TD
Start(["Start Validation"]) --> Prep["Prepare Manifest Artifacts"]
Prep --> LoadManifest["Load Manifest"]
LoadManifest --> HasResults{"Run Results Available?"}
HasResults --> |Yes| LoadResults["Load Run Results"]
HasResults --> |No| SkipResults["Skip Results Validation"]
LoadResults --> CrossCheck["Cross-check Manifest vs Run Results"]
SkipResults --> CrossCheck
CrossCheck --> End(["Validation Complete"])
```

**Diagram sources**
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)

**Section sources**
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)

### Jobs and Schedule Configuration Validation
- Jobs configuration validation checks job presets against dbt project metadata and environment settings.
- Schedule presets validation verifies cron-like scheduling configurations align with dbt models and partitions.

```mermaid
flowchart TD
JStart(["Validate Jobs/Schedules"]) --> LoadPresets["Load Job/Schedule Presets"]
LoadPresets --> MatchModels["Match Presets to Manifest Models"]
MatchModels --> EnvCheck["Validate Environment Settings"]
EnvCheck --> CompatCheck["Check Compatibility Constraints"]
CompatCheck --> JEnd(["Jobs/Schedules Validated"])
```

**Diagram sources**
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

**Section sources**
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

### Sensor Setup Validation
- Sensor detector validates partition-change sensor configuration against dbt manifest to ensure accurate impact propagation and watermark handling.

```mermaid
flowchart TD
SStart(["Validate Sensors"]) --> LoadManifest["Load Manifest"]
LoadManifest --> Detect["Detect Partition Change Sensors"]
Detect --> Compare["Compare with Manifest Metadata"]
Compare --> SEnd(["Sensors Validated"])
```

**Diagram sources**
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)

**Section sources**
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)

### Environment and Resource Validation
- Environment validation checks required environment variables and resource availability for dbt-dagsterizer operations.
- DBT resource validation ensures dbt CLI and profile settings are accessible and correct.

```mermaid
flowchart TD
EStart(["Validate Environment"]) --> CheckEnv["Check Required Env Vars"]
CheckEnv --> CheckDBT["Check DBT CLI/Profile Access"]
CheckDBT --> EEnd(["Environment Validated"])
```

**Diagram sources**
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)
- [dbt.py](file://src/dbt_dagsterizer/resources/dbt.py)

**Section sources**
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)
- [dbt.py](file://src/dbt_dagsterizer/resources/dbt.py)

## Dependency Analysis
The validation commands depend on:
- CLI composition for routing and argument parsing.
- Manifest preparation and loaders for dbt artifact validation.
- Jobs and schedules configuration modules for preset validation.
- Sensor detector for partition-change sensor validation.
- Environment and resource utilities for environment checks.

```mermaid
graph TB
V["Validation Commands"] --> MP["Manifest Prepare"]
V --> ML["Manifest Loader"]
V --> RC["Run Results Loader"]
V --> JC["Jobs Config"]
V --> SC["Schedule Presets"]
V --> SD["Sensor Detector"]
V --> ER["Env/Resource Utils"]
```

**Diagram sources**
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

**Section sources**
- [validation.py](file://src/dbt_dagsterizer/cli_parts/validation.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)

## Performance Considerations
- Manifest preparation and loading can be I/O intensive; cache prepared artifacts when iterating on validations.
- Batch validations across related components (jobs, schedules, sensors) to minimize repeated manifest loads.
- Limit optional run results validation to targeted periods to reduce overhead.

## Troubleshooting Guide
- Manifest not found or outdated: Re-run manifest preparation before validation.
- Missing environment variables: Set required environment variables as per environment utilities.
- DBT CLI/profile errors: Verify DBT CLI installation and profile configuration.
- Job/schedule preset mismatches: Align presets with manifest models and partitions.
- Sensor setup failures: Confirm sensor detector alignment with manifest metadata.

**Section sources**
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [env_utils.py](file://src/dbt_dagsterizer/env_utils.py)
- [dbt.py](file://src/dbt_dagsterizer/resources/dbt.py)
- [dbt_config.py](file://src/dbt_dagsterizer/jobs/dbt_config.py)
- [presets.py](file://src/dbt_dagsterizer/schedules/presets.py)
- [dbt_manifest.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/dbt_manifest.py)

## Conclusion
The validation commands in dbt-dagsterizer provide a cohesive set of checks spanning dbt project integrity, manifest consistency, configuration correctness, asset dependencies, schedule configurations, sensor setups, environment readiness, dependency resolution, and compatibility. By leveraging manifest preparation, loaders, configuration presets, and environment/resource utilities, these validations help maintain reliable and predictable DAG generation and orchestration.
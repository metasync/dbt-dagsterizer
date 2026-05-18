# Execution model and env propagation (Kubernetes)

This document explains where Dagster code executes in a Kubernetes deployment, and where environment variables must be configured so that jobs, sensors, and schedules can access credentials (for example StarRocks/dbt).

## At a glance

- Sensor/schedule evaluation runs in the code location (`dagster code-server`) environment.
- Job runs (assets/ops) run in separate Kubernetes pods when using `K8sRunLauncher`.
- If the same external system is accessed from both places, credentials must be available in both places.

## Components and where code runs

### Dagster daemon (control plane)

- Ticks schedules and sensors.
- Creates Dagster run records.
- Launches runs using the configured run launcher (for example `K8sRunLauncher`).
- Requests evaluation of user-code constructs (sensors/schedules) from the code location.

The daemon is primarily an orchestrator; it does not typically execute your repository’s sensor/schedule Python logic directly.

### Code location (user code, `dagster code-server`)

- Hosts your repository’s Python code (assets, jobs, sensors, schedules, resources).
- Evaluates sensors and schedules when requested by the daemon.
- Returns evaluation results to the daemon (for example `RunRequest`, `SkipReason`, and cursor updates).

Any sensor/schedule logic that talks to external systems (for example a detector querying StarRocks to compute watermarks) runs here, so credentials must be available in the code location process environment.

### Run pods (job execution)

When using `K8sRunLauncher`, each Dagster run executes in a separate Kubernetes pod. Asset/op code runs inside that run pod, and it needs credentials available in that pod environment.

Runs can be launched from the UI, sensors, schedules, backfills, or APIs, but the execution location is the same: the run pod.

## Env propagation in this project

### Code location env vars

In Luban GitOps, the code location `Deployment` typically uses `envFrom` to load a per-app ConfigMap/Secret. Those env vars are available to:

- sensors
- schedules
- any user-code executed at evaluation time (imports, resource construction, etc.)

### Run pod env vars (Kubernetes run pod env injection)

Run pods do not automatically inherit `envFrom` from the code location Deployment.

This project supports injecting a ConfigMap/Secret into run pods by setting `LUBAN_RUN_ENV_CONFIGMAP` and/or `LUBAN_RUN_ENV_SECRET` on the code location Deployment, which causes job definitions to include a `dagster-k8s/config` tag configuring `container_config.envFrom`.

See: [README.md](../../README.md) (“Kubernetes run pod env injection”).

## Credential checklist

- Put creds in the code location Deployment `envFrom` when code runs during evaluation time (sensors, schedules, and any import-time/resource construction logic).
- Ensure creds are injected into run pods when code runs during execution time (assets/ops in job runs).
- Configure both if both evaluation and execution talk to the same external system.

## Common patterns to watch

- Partition-change detector sensors: query StarRocks during sensor evaluation; require StarRocks env vars on the code location.
- Propagation sensors: usually read Dagster event logs and emit `RunRequest`s; may not require external credentials.
- Schedules: evaluation runs in the code location; any external calls in schedule functions need code location credentials.

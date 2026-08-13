# Fork Configuration

This document lists configuration keys introduced by the `karma-history` fork and any upstream keys with changed behavior/defaults.

Scope rule for this file:

- include only fork-added keys,
- include upstream keys only if fork changes their behavior or default.

## 1. New fork keys

New keys are grouped by their owning fork subsystem.

### `silenceEditor.maxPayloadBytes`

- Type: `int64` bytes
- Default: `0` (no additional Karma limit)
- CLI flag: `--silenceEditor.maxPayloadBytes`
- Environment variable: `SILENCEEDITOR_MAXPAYLOADBYTES`

Meaning:

- optional maximum size for a silence-editor submit request and for the fresh
  singular silence response used by Edit/Recreate verification;
- `0` lets Karma accept every payload size accepted by its HTTP server and by
  Alertmanager;
- a positive value should normally match Alertmanager's
  `--silences.max-silence-size-bytes` setting.

This limit does not apply to preview requests. Preview has its own fixed 64 KiB
request limit because it never carries an existing Alertmanager silence payload.

Validation:

- must be `>= 0`.

### `alertLifecycle.enabled`

- Type: `bool`
- Default: `true`
- CLI flag: `--alertLifecycle.enabled`
- Environment variable: `ALERTLIFECYCLE_ENABLED`

Meaning:

- enables persistent alert lifecycle storage subsystem (SQLite store, lifecycle transitions, housekeeping).

Operational note:

- if disabled, lifecycle persistence, retained-alert merge, Resolve, and lifecycle
  warning/stats behavior are inactive;
- silence autocomplete may still use current live alerts, but automatic preview is
  fail-closed without lifecycle storage. Keep this enabled for the complete compact
  editor contract.

### `alertLifecycle.sqlite.path`

- Type: `string`
- Default: `/var/lib/karma/alert-lifecycle.sqlite`
- CLI flag: `--alertLifecycle.sqlite.path`
- Environment variable: `ALERTLIFECYCLE_SQLITE_PATH`

Meaning:

- filesystem path to lifecycle SQLite database.

Container/Kubernetes note:

- default path is under `/var/lib/karma/` so you can mount a PVC directly to `/var/lib/karma`.
- directory is created automatically on startup.

Validation:

- must be non-empty.

### `alertLifecycle.retention.ttl`

- Type: `duration`
- Default: `720h` (30 days)
- CLI flag: `--alertLifecycle.retention.ttl`
- Environment variable: `ALERTLIFECYCLE_RETENTION_TTL`

Meaning:

- TTL cleanup window for stored lifecycle records.
- TTL is evaluated before max-record cleanup.

Validation:

- must be `> 0`.

### `alertLifecycle.retention.maxAlerts`

- Type: `int`
- Default: `100000`
- CLI flag: `--alertLifecycle.retention.maxAlerts`
- Environment variable: `ALERTLIFECYCLE_RETENTION_MAXALERTS`

Meaning:

- hard cap on stored alert records after TTL cleanup.
- if still above cap, oldest records by last state-change time are evicted first.

Validation:

- must be `>= 1`.

### `alertLifecycle.silenceHistory.retention.ttl`

- Type: `duration`
- Default: `720h` (30 days)
- CLI flag: `--alertLifecycle.silenceHistory.retention.ttl`
- Environment variable: `ALERTLIFECYCLE_SILENCEHISTORY_RETENTION_TTL`

Meaning:

- cleanup window for unreferenced closed silence revisions, episodes, retired
  sources, and old completed recheck-job data;
- current silence relations, queued/running/failed jobs, the latest completed job,
  and history referenced by retained alerts or evidence are protected regardless of
  age;
- this setting is independent from alert retention because silence evidence may
  have a different lifecycle from alert rows.

Validation:

- must be `> 0`.

### `alertLifecycle.housekeeping.incrementalVacuumInterval`

- Type: `duration`
- Default: `24h`
- CLI flag: `--alertLifecycle.housekeeping.incrementalVacuumInterval`
- Environment variable: `ALERTLIFECYCLE_HOUSEKEEPING_INCREMENTALVACUUMINTERVAL`

Meaning:

- interval for the complete background housekeeping cycle;
- one cycle runs immediately at worker startup and then at this interval;
- each cycle runs alert TTL cleanup, maxAlerts cleanup, reference-safe silence/job
  history cleanup, incremental vacuum, and one silence-recheck batch.

Validation:

- must be `> 0`.

## 2. Upstream keys changed by fork

At current fork stage, there are no upstream configuration keys with fork-modified defaults or semantics.

Fork keys are grouped under `alertLifecycle` and `silenceEditor`.

## 3. Example YAML

```yaml
alertLifecycle:
  enabled: true
  sqlite:
    path: /var/lib/karma/alert-lifecycle.sqlite
  retention:
    ttl: 720h
    maxAlerts: 100000
  silenceHistory:
    retention:
      ttl: 720h
  housekeeping:
    incrementalVacuumInterval: 24h

silenceEditor:
  # 0 means no additional Karma limit for submit/fresh-read payloads.
  maxPayloadBytes: 0
```

## 4. Related docs

- [Storage schema](SPEC/storage-schema.md)
- [Retention and vacuum](SPEC/retention-and-vacuum.md)
- [API contracts](SPEC/api-contracts.md)
- [User guide](README.md)
- [Compact silence editor](SILENCE_EDITOR.md)

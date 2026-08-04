# Fork Configuration

This document lists configuration keys introduced by the `karma-history` fork and any upstream keys with changed behavior/defaults.

Scope rule for this file:

- include only fork-added keys,
- include upstream keys only if fork changes their behavior or default.

## 1. New fork keys

All new keys are under `alertLifecycle`.

### `alertLifecycle.enabled`

- Type: `bool`
- Default: `true`
- CLI flag: `--alertLifecycle.enabled`
- Environment variable: `ALERTLIFECYCLE_ENABLED`

Meaning:

- enables persistent alert lifecycle storage subsystem (SQLite store, lifecycle transitions, housekeeping).

Operational note:

- if disabled, lifecycle persistence and related endpoints are inactive.

### `alertLifecycle.sqlite.path`

- Type: `string`
- Default: `./var/karma-alert-history.sqlite`
- CLI flag: `--alertLifecycle.sqlite.path`
- Environment variable: `ALERTLIFECYCLE_SQLITE_PATH`

Meaning:

- filesystem path to lifecycle SQLite database.

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

### `alertLifecycle.housekeeping.incrementalVacuumInterval`

- Type: `duration`
- Default: `24h`
- CLI flag: `--alertLifecycle.housekeeping.incrementalVacuumInterval`
- Environment variable: `ALERTLIFECYCLE_HOUSEKEEPING_INCREMENTALVACUUMINTERVAL`

Meaning:

- interval for periodic housekeeping cycle.
- each cycle runs in this order: TTL cleanup -> maxAlerts cleanup -> incremental vacuum.

Validation:

- must be `> 0`.

## 2. Upstream keys changed by fork

At current fork stage, there are no upstream configuration keys with fork-modified defaults or semantics.

Only new fork keys were added under `alertLifecycle`.

## 3. Example YAML

```yaml
alertLifecycle:
  enabled: true
  sqlite:
    path: ./var/karma-alert-history.sqlite
  retention:
    ttl: 720h
    maxAlerts: 100000
  housekeeping:
    incrementalVacuumInterval: 24h
```

## 4. Related docs

- `fork-docs/SPEC/storage-schema.md`
- `fork-docs/SPEC/retention-and-vacuum.md`
- `fork-docs/SPEC/api-contracts.md`

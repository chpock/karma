# Spec: Implementation Contracts and Integration Points

## 1. Objective

Keep fork behavior isolated while integrating the refactored state model end-to-end.

## 2. Minimal upstream touchpoints

The persistent lifecycle subsystem uses these primary upstream-owned integration
hooks:

- `cmd/karma/main.go`
  - lifecycle bootstrap/shutdown hooks
  - fork routes (Resolve, lifecycle stats/warning, and `/silence-editor/*`)
- `cmd/karma/timer.go`
  - post-poll lifecycle cycle hook
- `cmd/karma/views.go`
  - merged lifecycle dataset in response build path

All lifecycle logic remains in fork files/modules.

## 3. Data ownership

- Live snapshot ownership: Alertmanager collector + dedup pipeline.
- Lifecycle ownership: `internal/fork/alertlifecycle` (`alert_states` / `alert_events`).
- Silence reconciliation ownership: `internal/fork/alertlifecycle` (sources, immutable revisions, jobs, and cause history).
- Merge ownership: `cmd/karma/fork_alertlifecycle_merge.go`.

## 4. Filter and projection contract

- `@state` must operate on projected state (`alert.State`), not AM-only states.
- `@is_live` is lifecycle-aware and falls back to current Alertmanager presence for
  live API objects without persisted lifecycle dimensions.
- `@state` is not flagged as Alertmanager-only filter anymore.

## 5. Resolve contract

- Resolve endpoint delegates to store idempotent semantics.
- Non-live + already resolved is `200` no-op (`applied=false`).
- Live alerts return conflict.

## 6. Event and runtime logging contracts

Every logical alert mutation writes exactly one `alert_events` row with full old/new
tuple and reason metadata.

Application runtime logging is a separate operator-facing contract. It covers
operation routing, timing, queue execution, warnings, failures, and user actions and
is defined in `runtime-logging-contract.md`. A SQLite event row does not satisfy the
runtime logging contract, and a runtime log record does not replace durable event
history.

## 7. Testing contract

Required checks for this model:

- lifecycle store unit tests (`internal/fork/alertlifecycle`)
- filter tests (`internal/filters`) including `@is_live`
- model snapshot tests (`internal/models`)
- lifecycle integration tests (`cmd/karma` focused tests)
- UI TypeScript build
- root `make lint`, production Vite build, and focused compact/retained-editor tests
- Playwright visual coverage when modal layout or interaction changes

## 8. Write and transaction contract

- All lifecycle Store writers are serialized inside one process through one
  Store-owned SQLite writer connection. The writer boundary starts
  `BEGIN IMMEDIATE` before background regroup, housekeeping, poll, resolve, or
  silence recheck reads mutable lifecycle state.
- `Store.db` remains a multi-connection read pool. Read-only preview, autocomplete,
  merge, stats, and warning queries do not wait on `Store.writeMu` merely because a
  writer transaction exists.
- Writer transaction callbacks never own rollback. The helper rolls back exactly
  once on callback errors and panic paths, inserts an opaque operation token as its
  final statement, and attempts one commit. Commit completion has a typed outcome:
  `committed`, `not_committed`, or `unknown`.
- An uncertain commit discards/replaces the writer connection under writer
  ownership and reconciles the token through `lifecycle_writer_commits`. `unknown`
  blocks further mutations; a later durable `committed` result never re-runs the
  callback. Committed ledger rows are retained for 30 days by housekeeping.
- `journal_mode(WAL)` and `auto_vacuum(INCREMENTAL)` are persistent setup PRAGMAs
  executed and verified only through the owned writer connection before schema
  bootstrap. They are never run by read-pool connections or on connection
  replacement.
- Long-running regroup/recheck work is split into transactions so the writer lock is released between batches.
- Post-commit finalizers have an in-lock handoff for completion state and an
  after-unlock structured-log callback. Regroup consumes a committed phase result
  after delayed reconciliation; recheck writer infrastructure errors never become
  job failure state.
- Poll transaction order is source/silence reconciliation first, then alert state/events/live cause history, then commit.
- Poll runtime event payloads are captured in the transaction-local collector and are
  never reconstructed by querying rows with a timestamp after commit.
- A failed source refresh updates health only and preserves the last successful source set and fingerprint.
- SQLite busy timeout is 5 seconds as a defensive boundary for brief external/process-independent locks.
- The PID file is an owned lease: exclusive creation, an open handle, platform file
  identity, complete write/sync, and identity-checked release. Startup cleanup and
  normal shutdown release only a lease acquired by this process; release errors are
  joined with the originating lifecycle error.
- Regroup infrastructure stages and reason codes are typed and exact. Expected
  shutdown is carried as an explicit cancellation cause; external cancellation is
  warning-level and cannot be classified as normal shutdown by suffix matching.

## 9. Docs and schema contract

- Schema and API/UI contracts must be synced in `fork-docs/SPEC/`.
- The preview runtime supports only fresh schema v1. Any other database layout is
  rejected without modification.
- The schema layout fingerprint covers every application-owned `sqlite_schema` row,
  including tables, indexes, triggers, and views.

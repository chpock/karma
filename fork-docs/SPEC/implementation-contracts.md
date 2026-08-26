# Spec: Implementation Contracts and Integration Points

## 1. Objective

Keep fork behavior isolated while integrating the refactored state model end-to-end.

Problems 2 and 3 are implemented: immutable recheck input, durable retry/attempt history,
single scheduler ownership, and operator recovery use the contracts below.

## 2. Minimal upstream touchpoints

The persistent lifecycle subsystem uses these primary upstream-owned integration
hooks:

- `cmd/karma/main.go`
  - lifecycle bootstrap/shutdown hooks
  - fork routes (Resolve, lifecycle stats/warning,
    `/alerts-history/recheck/{jobID}/retry`,
    `/alerts-history/recheck/{jobID}/abandon`, and `/silence-editor/*`)
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
  silence recheck reads current semantic state for CAS. Recheck correctness inputs
  (rules, labels, source membership, and observation tokens) come from the immutable
  job snapshot, never from mutable current rows.
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
  callback. A transient attempt-tuple query leaves reconciliation pending and is never
  cached as an invariant. Committed ledger rows are retained for 30 days by housekeeping.
- A connection-invalidating generic-token or attempt-tuple lookup replaces the dedicated
  connection while preserving pending state. Generic-token absence is accepted only after
  the same finalizer confirms attempt-token absence; any present attempt is invariant.
- `journal_mode(WAL)` and `auto_vacuum(INCREMENTAL)` are persistent setup PRAGMAs
  executed and verified only through the owned writer connection before schema
  bootstrap. They are never run by read-pool connections or on connection
  replacement.
- Long-running regroup/recheck work is split into transactions so the writer lock is released between batches.
- Recheck batches create an opaque attempt token only after durable job selection. A
  committed attempt row is atomic with candidate progress; retry/failure bookkeeping is
  a separate writer transaction after rollback. The five retry waits are `1s`, `5s`,
  `30s`, `2m`, and `10m`; the sixth consecutive retryable failure becomes `failed`.
  Parent `attempt_count` must equal the durable ledger count before selection and
  idempotent bookkeeping. Failure replay retains the pre-attempt retry counters; it never
  derives the expected outcome from counters already changed by the same attempt. Every
  confirmed commit validates the complete attempt tuple, while the original commit cause
  remains available for BUSY allowlist classification.
- One cmd-owned recheck scheduler is the only automatic worker. Startup, committed poll
  job creation and operator actions own the ordinary wake; a delayed reconciled
  `recheck_batch` commit owns a separate reconciled wake. Reconciled `recheck_failure`
  returns through the already-active reconciliation timer and retained result without a
  finalizer wake. Immediate and reconciled committed failure use one operation-specific
  event finalizer; a terminal outcome emits retained selection/terminal events and releases
  its terminal-failure finalization barrier. A separate operator-action finalization barrier orders the applied
  event attempt before recheck execution and the operator wake. Ordinary recheck batches
  and empty/no-work commits never self-signal.
  Housekeeping is not a periodic wake source. The scheduler stores timer kind,
  owner, and absolute deadline; all wakes are coalesced while a process-local backoff or
  persisted head due timer is active. It is joined before Store close, and persisted
  `retry_wait` due times drive its timer. Typed acquisition, pre-attempt,
  failure-bookkeeping, reconciliation, and queue-read owners use independent process-local
  `1s`, `5s`, then capped `30s` backoff. Named success boundaries reset only the owners
  they prove successful; queue-read resets only after a successful head query. Commit
  uncertainty resets pre-attempt and advances only reconciliation. Unknown invariant
  failures terminate the owner until application restart. Only cancellation of the
  scheduler's own context is normal shutdown.
- Authenticated retry/abandon routes perform semantic CAS on `failed` jobs and append an
  idempotent operator-action row in the same transaction. They preserve immutable input,
  candidate progress, and attempt history. An exact replay returns the original result
  without another domain action/event/wake, while its committed writer invocation appends
  one operational `lifecycle_writer_commits` row. The request decoder rejects duplicate or
  case-variant keys, invalid UTF-8, null/non-string values, unknown fields, trailing JSON,
  and oversized bodies.
- Post-commit finalizers have an in-lock handoff for completion state and an
  after-unlock structured-log callback. Regroup consumes a committed phase result
  after delayed reconciliation. Recheck poll job-creation wakes belong to the original
  finalizer. Immediate and delayed retry/terminal failure share one event finalizer; a
  terminal outcome publishes its failure-finalization barrier under writer ownership,
  emits retained failure events after unlock, and releases the barrier before any waiting
  operator mutation. A committed operator publishes a separate action-finalization barrier,
  attempts `operator_action_applied` after unlock, releases that barrier, and only then
  makes its deferred scheduler wake runnable. A recheck batch
  owned by the active scheduler does not wake after an
  ordinary immediate commit; its retained finalizer wakes only after delayed
  cross-operation reconciliation. Recheck writer infrastructure errors never become job
  failure state.
- Poll transaction order is source/silence reconciliation first, then alert state/events/live cause history, then commit.
- Poll runtime event payloads are captured in the transaction-local collector and are
  never reconstructed by querying rows with a timestamp after commit.
- Recheck execution returns `RecheckBatchResult` with the ten approved dispositions;
  the Store emits committed domain events, while the cmd scheduler alone emits
  acquisition, retry-deferred, failed-head, and terminal infrastructure events.
  Persisted failed heads remain wakeable for operator Retry/Abandon and never emit a
  worker-selected or duplicate failure event.
- Lifecycle stats expose only the nonterminal FIFO head under `recheck.headJob`, and
  the warning endpoint exposes the ordered typed `issues[]` contract. Operator
  presentation never derives a job state from counters or error text.
- Poll metadata and post-commit event collection are best effort: queue depth,
  consistency snapshots, event payloads, and logging failures use a safe default or
  warning event and cannot roll back source reconciliation, alert transitions, or job
  creation.
- A failed source refresh updates health only and preserves the last successful source set and fingerprint.
- Poll reconciliation creates recheck jobs only after global and target transitions
  finish. A source-context job snapshots all final eligible scopes; an unchanged
  source context creates one targeted successor for the exact eligibility transitions.
- SQLite busy timeout is 5 seconds as a defensive boundary for brief external/process-independent locks.
- The PID file is an owned lease: exclusive creation, an open handle, platform file
  identity, complete write/sync, and identity-checked release. Startup cleanup and
  normal shutdown release only a lease acquired by this process; release errors are
  joined with the originating lifecycle error.
- Regroup infrastructure stages and reason codes are typed and exact. Expected
  shutdown is carried as an explicit cancellation cause; external cancellation is
  warning-level and cannot be classified as normal shutdown by suffix matching.

### 8.1 Recheck candidate classification

The worker loads pending candidates only by immutable job membership and terminal
outcome, never by current alert eligibility. Each candidate receives one terminal
outcome and increments exactly one corresponding parent counter in the same
transaction. Missing observation tokens are terminal and must never be promoted by an
`IS NULL` CAS branch.

Classification uses this exact priority:

1. missing current global alert;
2. missing required observation token;
3. stale global observation token;
4. missing current target for target-scoped work;
5. retired target;
6. stale target observation token;
7. already resolved;
8. already suppressed;
9. other ineligible semantic state;
10. missing or invalid pinned labels;
11. incomplete pinned source coverage;
12. no matching pinned rule;
13. promotion CAS; and
14. promoted.

If the promotion CAS affects zero rows, the worker re-reads global and target semantic
state in the same transaction and applies the same priority to classify the candidate
as stale observation or ineligible state. The constrained terminal `outcome` is the
complete machine-readable reason; free-form outcome reasons are not stored.

## 9. Docs and schema contract

- Schema and API/UI contracts must be synced in `fork-docs/SPEC/`.
- The preview runtime supports only fresh schema v1. Any other database layout is
  rejected without modification.
- The schema layout fingerprint covers every application-owned `sqlite_schema` row,
  including tables, indexes, triggers, and views.

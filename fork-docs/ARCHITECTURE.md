# Architecture: Persistent Alert Lifecycle

## 1. Scope

This document explains lifecycle architecture for fork-only persistence while keeping upstream divergence minimal.

Primary objective:

- retain and serve non-live alerts after they disappear from Alertmanager live snapshots.

Secondary objectives:

- deterministic behavior during partial outages,
- operationally safe retention and housekeeping,
- low-conflict upstream syncs.

## 2. Baseline upstream flow

Upstream pipeline is live-only:

1. poll Alertmanager,
2. deduplicate in-memory groups,
3. build API response,
4. UI renders current snapshot.

Without fork persistence, disappeared alerts vanish on next cycle.

## 3. Fork architecture components

1. **Live Snapshot Source**
   - existing upstream poll + dedup flow.
2. **Lifecycle Store**
   - SQLite current/event tables plus silence source, revision, recheck, and cause-history tables.
3. **Transition Engine**
   - applies poll-cycle updates and disappearance logic.
4. **Response Merger**
   - injects non-live persisted alerts into response dataset.
5. **Manual Resolve API**
   - explicit operator action with idempotent semantics for non-live alerts.
6. **Housekeeping and Regroup**
   - retention, vacuum, regroup consistency controls.
7. **Silence Reconciler and Recheck Worker**
   - stores the last successful silence set per source,
   - promotes eligible inactive alerts to suppressed using immutable job input.

## 4. State model architecture

Current-state row is modeled by tuple:

- `raw_state` (upstream fact),
- `is_live` (presence in current live snapshot),
- `is_suppressed` (normalized suppression fact),
- `is_resolved` (manual operator flag).

User-facing `state` is a deterministic projection over this tuple.

Why this is architecturally correct:

- separates upstream facts from fork semantics,
- prevents semantic collisions (suppressed vs inactive),
- supports silence-aware reclassification without overwriting the upstream-owned raw state.

## 5. Deterministic transition semantics

- Poll updates refresh live tuple and clear stale manual resolve when alert reappears.
- Disappearance transition (`is_live=true -> false`) runs only with full relevant upstream success coverage.
- Silence recheck can promote only `inactive -> suppressed`, and only when every relevant source matches.
- Manual resolve only affects non-live alerts and is idempotent.
- Every logical mutation writes one immutable `alert_events` row with old/new tuple.

## 6. Failure and uncertainty contract

Core rule: do not invent state when data is incomplete.

Operationally:

- partial upstream failures block disappearance inference,
- failed silence refresh preserves the last successful source set and fingerprint,
- lifecycle state is left unchanged until coverage is trustworthy.

## 7. SQLite writer contract

- Runtime mutations use one Store-owned SQLite writer connection and one writer
  ownership boundary. The boundary acquires `Store.writeMu` and starts
  `BEGIN IMMEDIATE` before the operation reads mutable lifecycle state.
- The shared `database/sql` pool remains multi-connection for read-only operations;
  preview, autocomplete, merge, stats, and warning reads are not serialized behind
  the writer mutex.
- The writer connection is acquired during Store startup, retained for the Store
  lifetime, closed before the read pool, and replaced under writer ownership when
  transaction completion leaves its driver state uncertain.
- Runtime setup applies and verifies `journal_mode=WAL` and
  `auto_vacuum=INCREMENTAL` only through the owned writer connection. The shared DSN
  contains connection-local settings and `_txlock=immediate`, but no write-capable
  setup PRAGMAs.
- Runtime write operations are serialized by the lifecycle Store.
- Every writer transaction has a durable opaque operation token. The token insert is
  the final statement before the single commit attempt; callback code cannot roll
  back independently.
- Commit errors produce one of three typed outcomes: committed, not committed, or
  unknown. Unknown outcome blocks mutations until the token is reconciled, preventing
  a second execution of an operation that may already have committed.
- Post-commit work is split into an in-lock `handoff` for synchronized completion
  state and an after-unlock observability `run` callback. Regroup consumes the
  captured committed phase result instead of treating `ErrWriterCommitReconciled`
  as a domain failure; recheck infrastructure errors never become job failures.
- Immediate and reconciled committed recheck failure bookkeeping share one
  operation-specific event finalizer. A terminal outcome publishes a terminal-failure
  finalization barrier under writer ownership, emits retained selection/terminal events,
  and releases the barrier after unlock. Retry/Abandon
  waits outside the writer mutex for that barrier. A successful action publishes a separate
  operator-action finalization barrier so recheck execution remains behind its applied
  event attempt and scheduler wake. This does not change the generic writer finalizer
  contract.
- `Store.Close` transitions through `closing`, waits for the writer mutex and all
  Store-owned workers, then closes the writer connection before the read pool.
- Regroup and silence recheck release the writer between batches, so poll/resolve/housekeeping can make progress.
- Poll runtime events are immutable transaction-local payloads emitted by the
  committed transaction's finalizer; no post-commit query by poll timestamp is used.
- Recheck worker results use the typed Problem 4 disposition contract. Store-owned
  committed domain events are distinct from cmd-owned scheduler infrastructure events;
  a failed FIFO head is represented once and remains wakeable for operator recovery.
- Stats and warning readers expose server-authoritative nested recheck state and
  ordered typed issues. Observability metadata collection is best effort and cannot
  alter the surrounding lifecycle transaction or operator result.
- SQLite also uses a 5-second busy timeout for brief locks outside the Store process.
- The PID path is an owned lease, not global mutable state: exclusive creation keeps
  an open handle and platform identity, and release validates that identity before
  removing the path. A pre-existing, symlinked, replaced, or unowned path is never
  removed by startup rollback or shutdown.
- PID lease identity capture is retryable: a transient failure preserves ownership and
  the open handle, while ownership loss clears the lease without touching a replacement.
  Unix and Windows release adapters join temporary/current/owned handle close errors.
- Unix and Windows release adapters use one shared platform-neutral release state machine;
  only identity extraction, current-path inspection, deletion, and handle operations are
  platform-specific primitives.
- Ownership-loss classification is phase-specific: retained-handle identity reads remain
  retryable unless definitive loss is marked, while current-path absence and confirmed
  reparse replacement can clear the lease.
- The managed test harness performs process-group cleanup before publishing its final
  `exit` marker. Cleanup and marker failures are persisted as `harness-error` before a
  waiter can report success; marker publication failure returns the protocol failure code.
- Regroup and housekeeping owners use explicit `stopped/running/stopping` arbitration
  with one cancellation and one worker join. Expected shutdown is an explicit cause;
  external cancellation is not silently downgraded to normal shutdown.
- Poll reconciliation persists source/silence identities and revisions before live alert cause members in the same transaction, so every cause FK points to an existing row.

## 8. Upstream-isolation contract

Fork logic lives in dedicated modules.

Persistent lifecycle integration hooks in upstream-owned files remain narrow:

- `cmd/karma/main.go` (bootstrap + routes),
- `cmd/karma/timer.go` (post-poll hook),
- `cmd/karma/views.go` (merged dataset path).

## 9. Observability model

Fork exposes lifecycle operational state through:

- `/alerts-history/stats` (rows, state counts, suppressed split, db/housekeeping/regroup telemetry),
- `/alerts-history/warning` (persistent warning contract for DB expected-state).

This avoids manual DB inspection for normal operations.

Runtime execution is additionally logged as structured `component`/`event` records.
The levels, required fields, noise rules, and redaction contract are defined in
`SPEC/runtime-logging-contract.md`. Durable `alert_events` and runtime logs are
separate: neither replaces the other.

## 10. Silence editor boundary

The compact editor is the only production silence form. It selects one logical
Alertmanager target, evaluates an automatic target-scoped preview, and submits one
create, edit, or recreate operation.

- Edit/Recreate read one deterministic physical member immediately before opening
  and again before mutation. An opaque revision token prevents known stale writes.
- A retained non-live rule may use the browser clock only for its initial card
  controls. The card opens without a remote request; fresh Alertmanager status is
  still authoritative before Edit/Recreate proceeds, and browser time never
  authorizes a mutation.
- One mutating POST is attempted per user action; Karma never retries another HA
  member after an ambiguous write.
- `proxy:true` is server-routed. `proxy:false` is browser-routed and therefore has
  only browser-console terminal audit after Karma issues the descriptor.
- The old editor and old silence Browser remain in source control but are not
  reachable from production UI.

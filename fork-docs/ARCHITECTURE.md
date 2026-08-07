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

- Runtime write operations are serialized by the lifecycle Store.
- Regroup and silence recheck release the writer between batches, so poll/resolve/housekeeping can make progress.
- SQLite also uses a 5-second busy timeout for brief locks outside the Store process.
- Poll reconciliation persists source/silence identities and revisions before live alert cause members in the same transaction, so every cause FK points to an existing row.

## 8. Upstream-isolation contract

Fork logic lives in dedicated modules.

Upstream-owned integration hooks remain narrow:

- `cmd/karma/main.go` (bootstrap + routes),
- `cmd/karma/timer.go` (post-poll hook),
- `cmd/karma/views.go` (merged dataset path).

## 9. Observability model

Fork exposes lifecycle operational state through:

- `/alerts-history/stats` (rows, state counts, suppressed split, db/housekeeping/regroup telemetry),
- `/alerts-history/warning` (persistent warning contract for DB expected-state).

This avoids manual DB inspection for normal operations.

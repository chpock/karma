# Spec: Alert Lifecycle State Model

## 1. Purpose

This spec defines the canonical lifecycle state model used by the fork.

## 2. Data model

Each stored alert has four state dimensions:

- `raw_state` - upstream Alertmanager state: `active|suppressed|unprocessed`
- `is_live` - whether the alert is present in the latest successful live snapshot
- `is_suppressed` - normalized suppression fact retained across a non-live episode
- `is_resolved` - manual flag set by operator action

User-facing `@state` is a projection over these dimensions.

## 3. Projection rules

Projection function:

1. if `is_live=true`:
   - `raw_state=active`, `is_suppressed=false` -> `active`
   - `raw_state=suppressed`, `is_suppressed=true` -> `suppressed`
   - `raw_state=unprocessed`, `is_suppressed=false` -> `unprocessed`
2. if `is_live=false`:
   - `is_resolved=true` -> `resolved`
   - else if `is_suppressed=true` -> `suppressed`
   - else -> `inactive`

This projection is deterministic and is used for API `state` and `@state` filtering.

## 4. Transition rules

### 4.1 Live poll updates

- Live snapshots update `raw_state`, force `is_live=true`, set `is_suppressed`
  exactly from `raw_state=suppressed`, and clear `is_resolved`.
- If alert reappears after being resolved, projection moves from `resolved` back to live projected state.

### 4.2 Disappearance handling

- Disappearance is evaluated only for currently live alerts.
- Transition to `is_live=false` is allowed only when all relevant upstreams for that alert were successfully polled in current cycle.
- With partial upstream failure, no inference is made (`is_live` remains unchanged).

### 4.3 Silence-aware promotion

- A persistent FIFO worker may promote only a non-live, unresolved, unsuppressed
  `inactive` alert to `suppressed`.
- Matching uses original Alertmanager labels and immutable silence-rule input pinned
  when the job was created.
- Every relevant source must provide complete suppression-compatible evidence.
- Once set for a non-live episode, `is_suppressed` is not cleared by a later silence
  edit, deletion, or expiry. A live reappearance recalculates it from Alertmanager.

### 4.4 Manual resolve

- Resolve is allowed only for non-live alerts (`is_live=false`).
- Resolve sets `is_resolved=true` (idempotent if already true).
- Live alerts cannot be resolved manually.

## 5. Invariants

1. **No synthetic resolve** - `is_resolved` is manual-only.
2. **No inference under uncertainty** - partial upstream failures block disappearance transitions.
3. **Suppressed semantics preserved** - non-live suppression is represented by
   `is_suppressed`, whether observed live or established by a recheck.
4. **Deterministic replay** - each mutation is logged with old/new tuple in append-only events.

## 6. History requirements

Each logical mutation writes one event row in the same transaction as current-state update, with:

- `old_state`, `new_state`
- `old_raw_state`, `new_raw_state`
- `old_is_live`, `new_is_live`
- `old_is_suppressed`, `new_is_suppressed`
- `old_is_resolved`, `new_is_resolved`
- `changed_at` (UTC RFC3339Nano)
- `change_source` (`poller|manual|silence`)
- `changed_by` (nullable)
- `reason_code` (nullable snake_case <= 64)
- `reason_text` (nullable)

No event row is written when the full tuple is unchanged.

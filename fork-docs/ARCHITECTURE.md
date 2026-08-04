# Architecture: Persistent Alert Lifecycle

## 1. Scope

This document defines the architecture for adding persistent lifecycle tracking to the karma fork while minimizing divergence from upstream.

Primary objective:

- Continue showing alerts after they disappear from live Alertmanager data.

Secondary objectives:

- Preserve upstream compatibility and reduce merge conflicts.
- Maintain deterministic behavior during partial outages.
- Keep implementation simple and operationally safe.

## 2. Existing upstream flow (baseline)

Upstream pipeline:

1. Poll Alertmanager instances.
2. Build per-upstream in-memory alert groups.
3. Deduplicate across upstreams.
4. Build API response for `POST /alerts.json`.
5. UI renders only current live data.

Important baseline properties:

- If an upstream poll fails, no trustworthy state transitions can be inferred.
- The system is optimized for live snapshot rendering.

## 3. Fork architecture overview

Fork introduces a sidecar lifecycle subsystem with SQLite persistence.

High-level components:

1. **Live Snapshot Source**
   - Existing upstream alert collection and dedup path.
2. **Lifecycle Store (SQLite)**
   - Current state per alert.
   - State transition history per alert.
3. **Transition Engine**
   - Computes lifecycle updates from successful live snapshots.
4. **Response Merger**
   - Merges live alerts with persisted non-live alerts for API response.
5. **Manual Resolve API**
   - User-triggered transition `inactive -> resolved`.
6. **Retention + Housekeeping**
   - TTL cleanup, cap by record count, periodic vacuum.

## 4. Integration boundaries (upstream isolation)

To keep merge friction low, fork changes should be concentrated in new fork-specific packages and small integration hooks:

- Add lifecycle module under a dedicated namespace (for example `internal/historyalerts` or similar).
- Keep upstream `internal/alertmanager` collection logic mostly unchanged.
- Add minimal hooks near:
  - end of successful collection cycle,
  - `alerts.json` response construction,
  - new fork API handlers and config wiring.

Avoid deep invasive rewrites in:

- alert mappers,
- upstream dedup internals,
- generic shared model semantics not required for the fork.

## 5. State ownership model

State ownership is split:

- **Live states** (`active`, `suppressed`, `unprocessed`) come from current Alertmanager data.
- **Derived non-live state** (`inactive`) is computed by fork transition rules.
- **Manual state** (`resolved`) is user-driven only.

The fork never auto-promotes anything to `resolved`.

## 6. Failure semantics and stability

Design rule: do not invent state when data is unavailable.

Therefore:

- If relevant upstream coverage is incomplete in a cycle, do not auto-transition alert to `inactive`.
- Manual resolve remains explicit and independent of upstream polling.

This ensures behavior is stable under network instability and partial outages.

## 7. Relevant upstream model

Relevant upstreams for an alert are based on the **latest successful live snapshot of that alert**, not on historical "ever seen" sets.

Why:

- Prevent stale historical memberships from blocking `inactive` forever after topology/config changes.
- Reflect current routing reality.

Operational effect:

- Alert can become `inactive` when absent across upstreams that are relevant now.

## 8. Storage mode and deployment model

Initial supported mode:

- **Single replica only** with local SQLite.

Not supported in this phase:

- Shared-write multi-replica with cross-node DB synchronization.

This limitation must stay explicit in user-facing and operator docs.

## 9. UI architecture contract

Default visibility in main grid:

- shown: `active`, `suppressed`, `unprocessed`, `inactive`
- hidden by default: `resolved`

Visual contract:

- `inactive` uses yellow styling.

`resolved` remains queryable/filterable for retrospective use, but is not in default view.

## 10. Operational observability

Fork adds storage observability endpoints and fields for:

- total alert records,
- total transition records,
- SQLite file size,
- configured TTL and limit,
- housekeeping timestamps/status.

Goal:

- operators can monitor growth and retention behavior without opening SQLite manually.

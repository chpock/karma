# Spec: Implementation Contracts and Integration Points

## 1. Objective

Define strict boundaries for how fork functionality integrates into upstream code paths.

## 2. Minimal integration points

Integration should be limited to a small set of touchpoints:

1. **Startup/bootstrap**
   - initialize lifecycle store and housekeeping scheduler.
2. **Post-poll processing**
   - after each successful cycle, run lifecycle transition processor.
3. **Alerts response assembly**
   - merge stored `inactive` alerts into response dataset.
4. **Fork API routes**
   - resolve endpoint.
   - stats endpoint.

Implemented touchpoints in upstream-owned files:

- `cmd/karma/main.go`
  - route hooks:
    - `POST /alerts/{id}/resolve`
    - `GET /alerts-history/stats`
    - `GET /alerts-history/warning`
  - startup/shutdown hooks:
    - `setupAlertLifecycleStorage()`
    - `closeAlertLifecycleStorage()`
- `cmd/karma/timer.go`
  - post-poll hook:
    - `runAlertLifecyclePollCycle(successfulUpstreams)`
- `cmd/karma/views.go`
  - dataset assembly hook:
    - `dedupedAlertsWithLifecycle(matchFilters)`

## 3. Non-invasive contract

Do not alter core upstream responsibilities unless required:

- alert fetching/parsing,
- Alertmanager proxying semantics,
- silence management behavior,
- existing API payloads unrelated to lifecycle.

## 4. Data ownership contract

- Live snapshot remains source of truth for live states.
- Lifecycle store is source of truth for `inactive/resolved` and transition history.
- Merge layer combines both for UI consumption.

## 5. Concurrency contract

In single-replica mode:

- one process owns writes to SQLite.
- writes from poller and resolve endpoint must use consistent transaction boundaries.

Recommended:

- short transactions,
- retry-on-busy with bounded backoff,
- idempotent transition writes.

## 6. Config contract

Fork-specific config must be namespaced and additive.

Suggested shape (exact key naming can be finalized during implementation):

- `alertLifecycle.enabled`
- `alertLifecycle.sqlite.path`
- `alertLifecycle.retention.ttl`
- `alertLifecycle.retention.maxAlerts`
- `alertLifecycle.housekeeping.incrementalVacuumInterval`

Defaults must match documented policy.

## 7. Testing contract

Required test layers:

- unit tests for storage and transition engine.
- API tests for resolve and stats endpoints.
- integration tests covering poll cycles with upstream failures.
- UI tests for inactive rendering and resolve action behavior.

## 8. Logging contract

Log key lifecycle operations at info/debug levels with structured context:

- cycle id/timestamp,
- transition counts by type,
- skipped inactive transitions due to incomplete upstream coverage,
- cleanup and vacuum results.

## 9. Database evolution contract

- Any SQLite schema change must follow `fork-docs/SPEC/db-migrations.md`.
- Every schema version bump requires an append-only migration note in
  `fork-docs/SPEC/db-migrations/`.

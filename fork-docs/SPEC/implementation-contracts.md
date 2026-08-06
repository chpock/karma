# Spec: Implementation Contracts and Integration Points

## 1. Objective

Keep fork behavior isolated while integrating the refactored state model end-to-end.

## 2. Minimal upstream touchpoints

Only these upstream-owned files are integration hooks:

- `cmd/karma/main.go`
  - lifecycle bootstrap/shutdown hooks
  - fork routes (`/alerts/{id}/resolve`, `/alerts-history/stats`, `/alerts-history/warning`)
- `cmd/karma/timer.go`
  - post-poll lifecycle cycle hook
- `cmd/karma/views.go`
  - merged lifecycle dataset in response build path

All lifecycle logic remains in fork files/modules.

## 3. Data ownership

- Live snapshot ownership: Alertmanager collector + dedup pipeline.
- Lifecycle ownership: `internal/fork/alertlifecycle` (`alert_states` / `alert_events`).
- Merge ownership: `cmd/karma/fork_alertlifecycle_merge.go`.

## 4. Filter and projection contract

- `@state` must operate on projected state (`alert.State`), not AM-only states.
- `@is_live` is lifecycle-aware and falls back to AM presence for legacy alerts without lifecycle fields.
- `@state` is not flagged as Alertmanager-only filter anymore.

## 5. Resolve contract

- Resolve endpoint delegates to store idempotent semantics.
- Non-live + already resolved is `200` no-op (`applied=false`).
- Live alerts return conflict.

## 6. Event logging contract

Every logical mutation writes exactly one `alert_events` row with full old/new tuple and reason metadata.

## 7. Testing contract

Required checks for this model:

- lifecycle store unit tests (`internal/fork/alertlifecycle`)
- filter tests (`internal/filters`) including `@is_live`
- model snapshot tests (`internal/models`)
- lifecycle integration tests (`cmd/karma` focused tests)
- UI TypeScript build

## 8. Docs and migration contract

- Schema and API/UI contracts must be synced in `fork-docs/SPEC/`.
- Any future schema bump remains subject to `fork-docs/SPEC/db-migrations.md` + dedicated migration note.

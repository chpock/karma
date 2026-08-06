# Karma History Fork

This directory contains fork-specific documentation for the `karma-history` branch.

Goal of this fork: keep historical alert lifecycle in persistent storage while preserving low-conflict syncs with upstream.

## User-facing behavior (what operators see)

Upstream karma is live-snapshot only: when alert disappears from Alertmanager, it disappears from UI.

This fork keeps alert lifecycle in SQLite and changes behavior as follows:

- Alerts can remain visible after disappearance from live Alertmanager data.
- Default grid shows: `active`, `suppressed`, `unprocessed`, `inactive`.
- `resolved` alerts remain stored but hidden by default.
- Operators can explicitly reveal resolved via filter (`@state=resolved`).
- Resolve action is manual; UI enables it for `inactive` alerts.
- Backend resolve semantics are idempotent for non-live alerts.

### Why it works this way

- **No guessing under uncertainty**: if upstream data is incomplete, lifecycle engine does not invent state transitions.
- **Clear operational semantics**:
  - `inactive` means non-live alert that still needs attention,
  - `resolved` means operator explicitly marked it resolved.
- **Suppressed is preserved semantically**:
  - non-live suppressed alerts stay suppressed in projection,
  - they are not mixed into inactive bucket.

### Important user-visible details

- `@state` filters projected user state.
- Additional `@is_live=true|false` allows separating current vs past suppressed alerts.
- Suppressed counters are split into `current` and `past` in UI badges/stats.
- A persistent warning banner appears if lifecycle DB is not in expected regroup state.

## Operator and deployment constraints

- Current deployment model: single replica with local SQLite.
- Lifecycle DB path default: `/var/lib/karma/alert-lifecycle.sqlite`.
- Retention defaults: TTL 30 days + max 100000 alerts.
- Housekeeping order: TTL cleanup -> maxAlerts cleanup -> incremental vacuum.

## Documentation map

- `ARCHITECTURE.md` - high-level architecture and integration boundaries.
- `CONFIGURATION.md` - fork-specific config keys and defaults.
- `IMPLEMENTATION_PLAN.md` - delivery record and maintenance policy.
- `FUTURE_FEATURES.md` - intentionally deferred scope.
- `SPEC/state-model.md` - canonical lifecycle semantics and projection rules.
- `SPEC/api-contracts.md` - API behavior and payload contracts.
- `SPEC/ui-contracts.md` - UI behavior and filtering/counter contracts.
- `SPEC/storage-schema.md` - SQLite schema and storage invariants.
- `SPEC/implementation-contracts.md` - integration boundaries, ownership, and test contracts.
- `SPEC/db-migrations.md` - migration policy.
- `SPEC/db-migrations/` - one document per schema-version change.
- `ADR/` - architecture decisions.

## Source-of-truth policy

For fork-only behavior, docs in `fork-docs/` are source of truth.

If code behavior changes, update matching spec/ADR in the same change.

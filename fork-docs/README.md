# Karma History Fork

This directory contains fork-specific documentation for the `karma-history` branch.

Goal of this fork: keep historical alert lifecycle in persistent storage while preserving low-conflict syncs with upstream.

## What users see

Upstream karma is live-snapshot only: when an alert disappears from Alertmanager, it disappears from UI.

This fork stores alert lifecycle in SQLite, so operators can work with both current and past alerts.

### States in the grid

- Default view shows `active`, `suppressed`, `unprocessed`, and `inactive`.
- `resolved` alerts stay in storage but are hidden by default.
- To show resolved alerts, add `@state=resolved`.

Why:

- `inactive` means the alert is no longer live but still needs attention.
- `resolved` means a person explicitly marked it resolved.
- Hidden-by-default resolved alerts keep the main grid focused on actionable alerts.

### Resolve behavior

- Resolve is a manual action from the alert menu.
- UI enables resolve for `inactive` alerts.
- If the same non-live alert is resolved again, backend treats it as a no-op (idempotent), not an error.

Why:

- Resolve is an operator decision, not an automatic guess.

### Suppressed behavior for non-live alerts

- If a suppressed alert becomes non-live, it stays `suppressed` (it does not become `inactive`).
- Non-live `inactive` alerts can become `suppressed` automatically when silence data proves they should be suppressed.
- Automatic promotion is strict: all relevant sources must support suppression; one non-matching source keeps it `inactive`.
- When source data is incomplete, no automatic suppression transition is made.
- Once non-live alert is suppressed, later silence edits/deletion/expiry do not flip it back to `inactive` during that same non-live episode.
- Suppression is recalculated only if alert becomes live again.

Why:

- This follows the "no guessing" rule and keeps suppression semantics stable across outages and silence churn.

### Filters, badges, and counters

- `@state` filters projected user state.
- `@is_live=true|false` separates current vs past alerts.
- Suppressed counters are split into `current` and `past` in badges and stats.

Why:

- Operators can quickly distinguish alerts firing now from historical context.

### Warning banner

- UI shows a persistent warning when lifecycle DB state is not yet consistent (for example, background reconciliation/recheck is unfinished or failed).
- Warning disappears only after DB returns to expected state.

Why:

- Inconsistency must be visible, never silent.

### Legacy coverage note

- Old rows that do not have raw matching labels are still visible in history.
- Those rows are excluded from automatic silence-based promotion until the alert appears live again and fresh raw labels are captured.

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
- `SPEC/phase2-silence-awareness-plan.md` - reviewed Phase 2 requirements, decisions, alternatives, and rationale.
- `SPEC/phase2-silence-awareness-schema.md` - exact schema v5 DDL, indexes, retention, CAS, and migration contract.
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

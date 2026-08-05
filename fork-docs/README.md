# Karma History Fork

This directory contains fork-specific documentation for the `karma-history` branch.

The goal of this fork is to extend upstream karma with persistent alert lifecycle storage and UI support for historical (non-live) alerts, while keeping changes as isolated as possible from upstream code to reduce merge conflicts during future syncs.

## Why this fork exists

Upstream karma shows alerts that are currently visible in Alertmanager.
When an alert disappears from Alertmanager responses, it disappears from UI.

This fork adds:

- Persistent storage of alert lifecycle records in SQLite.
- A new automatic lifecycle state `inactive` for alerts that are no longer present in live Alertmanager data.
- A manual state `resolved` set explicitly by a UI action.
- Full state transition history with timestamps.
- Retention and storage housekeeping (TTL, record limit, VACUUM policy).

## Design constraints agreed for this fork

- Keep behavior stable and predictable, do not infer state when data is missing.
- Do not add new dedup/grouping logic now.
- Alert identity must follow existing core/UI behavior.
- `resolved` is manual-only (no auto-resolve).
- Default view includes all states except `resolved`.
- `inactive` is shown with yellow UI styling.
- Initial deployment mode is single-replica only (local SQLite).

## Document map

- `ARCHITECTURE.md` - high-level architecture and integration boundaries.
- `CONFIGURATION.md` - fork-specific config keys and defaults.
- `IMPLEMENTATION_PLAN.md` - phased implementation and verification plan.
- `FUTURE_FEATURES.md` - intentionally deferred scope.
- `SPEC/` - technical contracts by area.
- `SPEC/db-migrations.md` - migration policy and workflow for schema changes.
- `SPEC/db-migrations/` - one document per schema version change.
- `ADR/` - architecture decision records.

## Source of truth policy

For fork-only behavior, documents in this directory are the source of truth.
If code behavior changes, update the corresponding spec and ADR in the same change.

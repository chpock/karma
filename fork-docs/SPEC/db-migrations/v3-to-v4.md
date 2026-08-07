# Migration v3 -> v4 (Historical Preview Reset Boundary)

## Summary

- Version change: `schema_version: 3 -> 4`
- Date: 2026-08-06
- Status: historical exception; no row-preserving migration was implemented
- Purpose: replace the preview state-only model with normalized `raw_state`,
  `is_live`, and `is_resolved` lifecycle dimensions.

## Important limitation

The v4 bootstrap creates/uses:

- `alert_states`
- `alert_events`

It does **not** copy or rename rows from the earlier preview tables:

- `alerts_current`
- `alert_state_history`

The earlier design explicitly treated this state-model refactor as a preview reset
where old DB reconstruction was not required. The old tables may remain physically
present but are ignored by v4 runtime code.

This document records actual behavior; it does not claim that a preserving upgrade
or an old-DB fixture test exists.

## Effective v4 schema changes

`alert_states` represents:

- upstream `raw_state`,
- current `is_live`,
- manual `is_resolved`,
- projected `state`,
- nullable resolve provenance.

`alert_events` stores old/new raw/live/resolved dimensions plus stable reason
metadata.

## Operator impact

- A deployment that still depends on data only in v3 preview tables must not assume
  v4/v5 copied that data.
- Recreating the preview DB was the supported transition at the time.
- No newer migration may silently treat ignored legacy rows as successfully
  migrated data.

## v5 precondition

The planned v4 -> v5 migration must inspect for populated legacy preview tables.
If rows exist there without an explicit preserving conversion, migration aborts
before any schema/version change and reports a clear operator error.

This is a historical exception to the current migration policy. Schema changes
after v4 must follow `fork-docs/SPEC/db-migrations.md` completely.

# Migration v2 -> v3

## Summary

- Version bump: `schema_version: 2 -> 3`
- Date: 2026-08-06
- Purpose: add non-blocking background regroup support when grouping-related
  config changes, plus explicit DB expected-state signaling for UI warnings.

## Schema changes

No new SQL tables or columns.

This migration is metadata-only and introduces new metadata keys in `metadata`
table:

- `grouping_fingerprint`
- `regroup_status`
- `regroup_error`
- `regroup_processed`
- `regroup_total`
- `regroup_started_at`
- `regroup_finished_at`
- `regroup_target_fingerprint`
- `regroup_cursor_rowid`

## Runtime migration behavior

On startup:

1. Ensure base schema still exists.
2. Seed regroup metadata keys with defaults using `INSERT OR IGNORE`.
3. Set `metadata.schema_version='3'`.
4. Start background regroup worker (non-blocking startup).

## Data semantics

- DB can temporarily be in non-expected grouping state while background regroup
  is running.
- UI receives persistent warning state from `/alerts-history/warning` until
  regroup reaches `ready` and stored fingerprint matches current config
  fingerprint.

## Compatibility notes

- Forward: newer binary auto-seeds metadata for v2 DB and starts regroup.
- Idempotence: repeated startup keeps metadata consistent.
- Operator signal: warning is persistent in UI while DB state is not expected.

## Validation performed

- lifecycle store tests pass including regroup-to-config flow.
- targeted cmd lifecycle tests pass.
- UI build passes with warning component and stats extensions.

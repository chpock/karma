# Migration v1 -> v2

## Summary

- Version bump: `schema_version: 1 -> 2`
- Date: 2026-08-06
- Purpose: persist group label snapshots for lifecycle-only groups so UI group
  titles remain identical to live-group titles after alerts become inactive.

## Schema changes

Table `alert_states` (table name in schema v1 before state-refactor rename):

- add column:
  - `group_labels_json TEXT NOT NULL DEFAULT '{}'`

No table drops.
No data rewrite required.

## Runtime migration behavior

On startup:

1. Ensure base tables/indexes exist.
2. Attempt `ALTER TABLE alert_states ADD COLUMN group_labels_json ...`.
3. If column already exists, continue (idempotent path).
4. Upsert `metadata.schema_version='2'`.

## Data semantics

- For existing rows upgraded from v1, `group_labels_json` defaults to `{}`.
- New poll-cycle writes populate `group_labels_json` from live group labels.
- Merge path uses `group_labels_json` when rebuilding lifecycle-only groups.

## Compatibility notes

- Forward: newer binary upgrades v1 DB automatically.
- Idempotence: startup may run migration check multiple times safely.
- Rollback: code rollback to pre-v2 binary is not guaranteed to preserve
  lifecycle-only group title parity (column remains but old code ignores it).

## Validation performed

- internal lifecycle tests pass.
- cmd/karma lifecycle merge tests confirm lifecycle-only groups restore expected
  group labels in headers.

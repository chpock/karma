# Migration v4 -> v5

## Summary

- Version bump: `schema_version: 4 -> 5`
- Planning date: 2026-08-07
- Status: implemented
- Purpose: add Phase 2 silence-aware inactive-alert reclassification, exact
  suppression-cause history, immutable silence revisions, a mutable last-successful
  source set, and persistent FIFO jobs with immutable per-job input.

## Authoritative design

- Decisions and rationale: `../phase2-silence-awareness-plan.md`
- Exact DDL and migration order: `../phase2-silence-awareness-schema.md`

This migration record describes the implemented schema version 5 transition.

## Existing-table changes

`alert_states` is rebuilt to:

- add `is_suppressed`,
- add nullable `suppression_observed_at`,
- add nullable source `match_labels_json`,
- add canonical `relevant_sources_json` while keeping legacy-compatible `relevant_upstreams_json`,
- enforce tuple/projection CHECK constraints.

`alert_events` is rebuilt to add:

- `old_is_suppressed`,
- `new_is_suppressed`,
- Phase 2 source/reason values without embedding silence IDs in JSON reason text.

## New tables

- `alertmanager_sources`
- `silence_rules`
- `silence_rule_episodes`
- `silence_revisions`
- `alertmanager_source_silences`
- `silence_recheck_jobs`
- `silence_recheck_job_rules`
- `alert_suppression_cause_history`
- `alert_suppression_cause_members`

## Data migration semantics

- original matcher labels cannot be reconstructed from transformed v4
  `labels_json`; migrated `match_labels_json` is therefore NULL,
- migrated rows with unknown matcher labels are excluded from Phase 2 and surfaced
  through persistent warning/stats,
- `is_suppressed` is deterministically backfilled from recoverable current
  projected/raw state,
- `suppression_observed_at` remains NULL when exact history cannot establish it,
- existing relevant source Names are mapped through current configuration; removed
  unknown Names become retired source rows with NULL cluster identity,
- currently suppressed legacy alerts receive `legacy_unknown` cause-history records
  rather than invented silence IDs.

## Transaction and rollback contract

- migration runs in one transaction with foreign keys enabled,
- preflight aborts if populated legacy v3 preview tables are present but not
  represented in canonical v4 tables,
- child `alert_events` is removed before parent `alert_states` during table swap,
- Phase 2 tables are created only after final rebuilt table names exist,
- `PRAGMA foreign_key_check` must return no rows before version update,
- `metadata.schema_version` changes to `5` only as the final migration write,
- any failure rolls back schema and data to effective v4.

## Operator impact

- no automatic DB deletion or rebuild,
- historical v4 alerts remain visible,
- Phase 2 coverage for migrated non-live rows is incomplete until live reappearance
  supplies original labels, unless the operator explicitly chooses to recreate the
  preview DB,
- persistent warning makes that incomplete coverage visible.

## Required validation before status becomes implemented

- fresh v5 bootstrap test,
- real v4 fixture -> v5 upgrade test,
- injected failure rollback test,
- populated legacy v3 preview-table preflight-abort test,
- foreign-key and projection-constraint tests,
- cause-set cardinality/effective-query tests,
- current-source diff and failed-refresh preservation tests,
- pinned per-job input, FIFO restart/failover, and idempotency tests,
- stale `last_seen_at` CAS integration test,
- housekeeping/reference-protection tests.

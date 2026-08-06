# Lifecycle Feature Delivery Record

## 1. Purpose

This document is no longer a forward plan. It is a compact delivery record for the lifecycle feature implementation and verification.

Detailed behavioral and technical contracts are now the source of truth in:

- `fork-docs/SPEC/state-model.md`
- `fork-docs/SPEC/api-contracts.md`
- `fork-docs/SPEC/ui-contracts.md`
- `fork-docs/SPEC/storage-schema.md`
- `fork-docs/SPEC/implementation-contracts.md`

## 2. Delivered capability

Implemented in fork scope:

- persistent lifecycle storage in SQLite,
- tuple-based state model (`raw_state`, `is_live`, `is_resolved`) with projected user-facing state,
- immutable event history (`alert_events`) with old/new tuple and reason metadata,
- post-poll transition engine with strict upstream-coverage gating,
- merge layer for non-live persisted alerts,
- idempotent manual resolve endpoint for non-live alerts,
- UI support for projected states and suppressed `current/past` split,
- housekeeping (TTL -> limit -> incremental vacuum),
- storage and warning observability endpoints.

## 3. Deferred items

Deferred features are tracked in `fork-docs/FUTURE_FEATURES.md`.

Most relevant deferred item for lifecycle model:

- silence-aware reclassification layer (inactive -> suppressed based on silence data).

## 4. Verification record

Expected verification set for this feature family:

- backend unit/integration tests for lifecycle store and merge/resolve paths,
- filter tests including `@is_live`,
- model snapshot tests,
- UI TypeScript build validation.

## 5. Maintenance rule

When lifecycle behavior changes:

1. update affected specs in `fork-docs/SPEC/`,
2. add migration record when schema version changes,
3. keep this file as high-level delivery ledger only.

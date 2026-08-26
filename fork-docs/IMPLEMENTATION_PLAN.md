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
- four-dimensional state model (`raw_state`, `is_live`, `is_suppressed`,
  `is_resolved`) with projected user-facing state,
- immutable event history (`alert_events`) with old/new tuple and reason metadata,
- post-poll transition engine with strict upstream-coverage gating,
- merge layer for non-live persisted alerts,
- idempotent manual resolve endpoint for non-live alerts,
- UI support for projected states and suppressed `current/past` split,
- housekeeping (alert TTL -> limit -> reference-safe completed-job cleanup and
  silence-history TTL cleanup -> incremental vacuum),
- D21 completed-job retention documentation is reconciled under Problem 7, with no
  remaining problem-specific findings and its final integrated documentation gate passed;
  runtime cleanup remains reference-based and next-housekeeping while
  silence-history TTL remains independent,
- Problem 10 housekeeping documentation has no remaining problem-specific findings and
  its final integrated documentation gate passed: housekeeping does not execute or wake
  recheck work; ADR-007 and the cmd/karma scheduler own recheck execution,
- storage and warning observability endpoints.
- per-target lifecycle state and topology retirement,
- silence source/revision/episode history and HA consistency tracking,
- persistent FIFO silence recheck with exact causes; immutable target-scoped candidate
  membership, pinned observation input, and terminal outcomes are implemented and
  verified under Problem 2; Problem 3 adds durable attempts, bounded
  retry, token reconciliation, one scheduler owner, and audited operator recovery;
  the Problem 3 implementation phase and selected-error/shutdown precedence are
  implemented and independently accepted,
- effective suppression evidence on retained past-alert cards using the ordinary
  silence-rule presentation and actions,
- compact create/edit/recreate silence editor with automatic preview,
- structured runtime logging with redaction and edge-sensitive levels; Problem 4 recheck
  result projection, event ownership, strict APIs, and operator presentation include
  authoritative authentication-loss invalidation; its final focused/UI gate passed and the
  implementation is independently accepted. Problem 9 writer-operation registry/evidence
  also passed its focused/logging gate and is independently accepted;
- Problem 5 end-to-end evidence passed its final integrated gate for its independent exact
  deterministic and recorded-replay models and is independently accepted.
- PID lease recovery and managed-harness finalization with deterministic retry,
  ownership-loss, close-error, cleanup-order, and marker-failure evidence are
  implemented and independently accepted under Problem 6.

## 3. Deferred items

Deferred features are tracked in `fork-docs/FUTURE_FEATURES.md`.

The most relevant deferred items are multi-replica shared storage, retrospective
history UI, silence annotations, and a production silence-rule Browser. Silence-aware
`inactive -> suppressed` reclassification is implemented.

## 4. Verification record

Expected verification set for this feature family:

- backend unit/integration tests for lifecycle store and merge/resolve paths,
- filter tests including `@is_live`,
- model snapshot tests,
- UI TypeScript build validation.

## 5. Maintenance rule

When lifecycle behavior changes:

1. update affected specs in `fork-docs/SPEC/`,
2. keep the single fresh schema v1 definition synchronized with storage docs,
3. keep this file as high-level delivery ledger only.

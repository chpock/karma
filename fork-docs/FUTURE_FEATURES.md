# Future Features (Out of Current Scope)

This document tracks intentionally deferred features.

## 1. Multi-replica and shared storage

Deferred:

- running multiple karma replicas with shared writable lifecycle storage.
- distributed locking and conflict control.

Current scope remains single-replica local SQLite only.

## 0. Silence-aware state reclassification

Deferred:

- second-layer lifecycle logic that can reclassify some non-live alerts based on fresh silence data.
- specifically, inactive-to-suppressed reclassification path driven by silence synchronization.

Current scope:

- foundational state model (`raw_state`, `is_live`, `is_resolved`) is implemented.
- no silence-driven reclassification logic is active yet.

## 2. Advanced alert identity and dedup strategy

Deferred:

- custom identity keys independent from current core/UI identity.
- configurable dedup/grouping behavior for historical entries.

Current scope:

- keep alert identity exactly aligned with existing behavior.

## 3. Rich retrospective UI

Deferred:

- dedicated resolved/inactive timeline pages.
- state transition charts and analytics views.
- full per-alert history explorer in UI.

Current scope:

- use existing grid and filtering with default hiding of resolved.

## 4. Resolve authorization policy extensions

Deferred:

- role/ACL based restrictions for manual resolve.
- policy-driven resolve approval workflows.

Current scope:

- use existing auth context if present, allow operation without auth.

## 5. Acknowledgement workflow for active alerts

Deferred:

- ACK state/overlay for temporarily hiding active alerts.
- transitions involving ACK and resolved semantics.

Current scope:

- manual resolve only from inactive.

## 6. Extended audit semantics

Deferred:

- richer actor metadata model beyond nullable username.
- signed audit entries and immutable external audit streaming.

Current scope:

- nullable `changed_by` with explicit `NULL` semantics.

## 7. Storage backend alternatives

Deferred:

- PostgreSQL/MySQL backends.
- cloud-native managed persistence options.

Current scope:

- SQLite only.

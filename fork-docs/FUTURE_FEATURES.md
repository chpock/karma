# Future Features (Out of Current Scope)

This document tracks intentionally deferred features.

## 1. Silence rule annotations

Deferred:

- display Alertmanager silence annotations in the compact editor;
- create, edit, and remove annotation key/value pairs;
- preserve annotations through Edit and Recreate;
- support annotations in both server-routed and browser-routed submissions;
- include annotations in validation, optimistic-concurrency behavior, API types,
  and regression tests;
- verify compatibility with Alertmanager versions that support silence annotations
  (`0.32+`).

Current scope:

- Karma does not support Alertmanager silence annotations;
- the compact editor does not display or edit them;
- Create does not send them;
- Edit and Recreate may remove or overwrite annotations already stored in
  Alertmanager;
- this data-loss behavior is an explicitly accepted limitation of the current
  preview implementation, not a current defect.

## 2. Multi-replica and shared storage

Deferred:

- running multiple karma replicas with shared writable lifecycle storage.
- distributed locking and conflict control.

Current scope remains single-replica local SQLite only.

## 3. Advanced alert identity and dedup strategy

Deferred:

- custom identity keys independent from current core/UI identity.
- configurable dedup/grouping behavior for historical entries.

Current scope:

- keep alert identity exactly aligned with existing behavior.

## 4. Rich retrospective UI

Deferred:

- dedicated resolved/inactive timeline pages.
- state transition charts and analytics views.
- full per-alert history explorer in UI.

Current scope:

- use existing grid and filtering with default hiding of resolved.

## 5. Resolve authorization policy extensions

Deferred:

- role/ACL based restrictions for manual resolve.
- policy-driven resolve approval workflows.

Current scope:

- use existing auth context if present, allow operation without auth.

## 6. Acknowledgement workflow for active alerts

Deferred:

- ACK state/overlay for temporarily hiding active alerts.
- transitions involving ACK and resolved semantics.

Current scope:

- manual resolve is exposed in the current UI only for inactive alerts; backend
  accepts any non-live alert.

## 7. Extended audit semantics

Deferred:

- richer actor metadata model beyond nullable username.
- signed audit entries and immutable external audit streaming.

Current scope:

- nullable `changed_by` with explicit `NULL` semantics.

## 8. Storage backend alternatives

Deferred:

- PostgreSQL/MySQL backends.
- cloud-native managed persistence options.

Current scope:

- SQLite only.

## 9. Silence rule browser

Deferred:

- exposing a UI for searching, sorting, inspecting, editing, recreating, and
  deleting existing Alertmanager silence rules.

The retained source, missing product decisions, and known risks are documented in
[Future Silence Browser](SPEC/future-silence-browser.md).

Current scope:

- the compact silence editor has no Browse tab or hidden Browser entry point;
- existing external create/edit/recreate actions open the compact editor directly.

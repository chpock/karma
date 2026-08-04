# Spec: UI Contracts

## 1. Scope

This document defines user-facing behavior for lifecycle states and resolve actions.

## 2. Default visibility

Main grid default must include alerts in states:

- `active`
- `suppressed`
- `unprocessed`
- `inactive`

`resolved` alerts remain stored and queryable, but hidden by default.

## 3. State colors

Visual mapping:

- `active` - existing danger/red style.
- `suppressed` - existing success/green style.
- `unprocessed` - existing neutral style.
- `inactive` - yellow style.
- `resolved` - hidden by default (style only needed if explicitly displayed in future).

## 4. Resolve action

The alert menu adds action "Resolve" for `inactive` alerts.

Rules:

- action is always rendered in the alert menu, but enabled only when alert state is `inactive`.
- action calls `POST /alerts/{id}/resolve`.
- on success, alert disappears from default grid view.
- if filter currently includes `resolved`, alert remains visible with updated state.

## 5. Authentication behavior

Current phase supports both:

- authenticated environments,
- unauthenticated environments.

If auth is disabled, resolve action remains available.

## 6. Nullable actor display

If UI surfaces transition actor info and `changed_by` is `NULL`, it should render as literal `NULL` with neutral/gray styling to indicate true null value.

## 7. Filtering contract

`@state` filter must support:

- `active`
- `suppressed`
- `unprocessed`
- `inactive`
- `resolved`

Autocomplete and validation must include new values.

Resolved visibility rule:

- `resolved` alerts are merged into response only when `@state=resolved` is present.

Quick access rule:

- filter history menu includes a quick action that adds `@state=resolved`.

Operator visibility rule:

- settings modal includes a fork storage stats section backed by `GET /alerts-history/stats`.

## 8. Non-goals in this phase

- Dedicated retrospective page for resolved alerts.
- Complex lifecycle timeline UI.
- Advanced resolve/ack workflows.

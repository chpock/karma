# Spec: Alert Lifecycle State Model

## 1. Purpose

This spec defines alert lifecycle states, transitions, invariants, and merge behavior for the fork.

## 2. States

### 2.1 Live states (from Alertmanager)

- `active`
- `suppressed`
- `unprocessed`

These are derived from current live data as in upstream behavior.

### 2.2 Fork lifecycle states

- `inactive` - alert is absent from current live data after required successful upstream checks.
- `resolved` - manually set from UI only.

## 3. Transition rules

### 3.1 Live-driven transitions

- Any stored alert that is present in live snapshot must move to a live state according to current snapshot.
- If a `resolved` alert reappears in live data, it transitions back to live state (typically `active`).

### 3.2 Auto-inactive transition

- Auto-transition to `inactive` is allowed only if:
  1. alert is absent from live snapshot in current cycle, and
  2. all relevant upstreams for this alert were successfully polled in current cycle.

- If (2) is not satisfied, no state downgrade occurs.

### 3.3 Manual resolve transition

- `resolved` can be set only by explicit API/UI action.
- Allowed source state: `inactive` only.
- Disallowed transitions by manual resolve:
  - `active -> resolved`
  - `suppressed -> resolved`
  - `unprocessed -> resolved`

## 4. Merge output contract

When building `alerts.json` response:

- Include live alerts (`active/suppressed/unprocessed`) from current snapshot.
- Include persisted `inactive` alerts.
- Exclude `resolved` by default.
- Allow filters to include/exclude any state explicitly.

## 5. Invariants

The system must satisfy:

1. **No synthetic resolve**: `resolved` is never auto-assigned.
2. **No inference under uncertainty**: missing upstream data blocks auto-inactive.
3. **Deterministic replay**: current alert state is derivable from state history.
4. **Identity parity**: alert identity must match existing core/UI identity behavior.

## 6. State history requirements

Every state transition must append one transition row with:

- alert identity
- previous state
- new state
- transition timestamp (UTC)
- transition source (`poller` or `manual`)
- actor (nullable; `NULL` when no authenticated user context)

No transition row is required if state did not change.

## 7. Actor nullability

For manual resolve:

- If authentication is enabled and user context exists, set actor to username.
- If authentication is disabled or unavailable, store actor as `NULL`.

Rationale:

- avoid conflating explicit usernames with implicit anonymous placeholders.

## 8. Time semantics

Use UTC timestamps for all persisted transition times.

`last_seen` semantics:

- Updated when alert appears in live snapshot.
- Used by TTL retention.

`last_state_change_at` can be derived from transition history (latest row per alert).
Persisting a denormalized copy is optional optimization, not a behavioral requirement.

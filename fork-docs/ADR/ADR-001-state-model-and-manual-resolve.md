# ADR-001: State Model and Manual Resolve Semantics

- Status: Accepted
- Date: 2026-08-03

## Context

Upstream karma is live-snapshot based and does not persist lifecycle states for alerts that disappear from Alertmanager.

Fork requires persisted lifecycle with explicit operator control over final resolution state.

## Decision

Adopt lifecycle states:

- Live: `active`, `suppressed`, `unprocessed`
- Fork: `inactive`, `resolved`

Resolution semantics:

- `resolved` is manual-only via UI/API.
- Automatic transition to `resolved` is forbidden.
- Manual resolve is allowed only from `inactive`.

If a resolved alert reappears live, it transitions back to live state.

## Consequences

- Clear distinction between data-driven inactivity and human acknowledgement.
- Better retrospective traceability.
- Requires resolve API and UI action.

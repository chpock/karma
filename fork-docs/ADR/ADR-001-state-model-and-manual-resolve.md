# ADR-001: State Model and Manual Resolve Semantics

- Status: Accepted
- Date: 2026-08-03

## Context

Upstream karma is live-snapshot based and does not persist lifecycle states for alerts that disappear from Alertmanager.

Fork requires persisted lifecycle with explicit operator control over final resolution state.

## Decision

Adopt tuple-based lifecycle model:

- `raw_state` (`active|suppressed|unprocessed`) - upstream fact,
- `is_live` - live presence flag,
- `is_suppressed` - normalized suppression fact,
- `is_resolved` - manual operator flag.

User-facing `state` is a deterministic projection of this tuple.

Resolve semantics:

- `is_resolved` is manual-only via UI/API.
- automatic resolve is forbidden.
- resolve is allowed only for non-live alerts (`is_live=false`).
- resolve endpoint is idempotent for already resolved non-live alerts.

If resolved alert reappears live (`is_live=true`), resolved flag is cleared by poll cycle.

## Consequences

- Clear separation of upstream state fact vs lifecycle semantics.
- Suppressed alerts can remain suppressed even when non-live (no forced suppressed->inactive mapping).
- Silence recheck can establish suppression without overwriting upstream-owned
  `raw_state`; live reappearance recalculates suppression from the new upstream fact.
- Better retrospective traceability through tuple-aware event history.
- Requires additive API/UI fields (`rawState`, `isLive`, `isResolved`) and filter extension (`@is_live`).

# Implementation Plan

## 1. Goal

Deliver persistent alert lifecycle storage with minimal upstream disruption and clear rollback boundaries.

## 2. Guiding principles

- Keep fork-specific logic isolated in dedicated modules.
- Add small integration hooks instead of large rewrites.
- Preserve current upstream behavior for live alerts.
- Fail safe when upstream coverage is incomplete.

## 3. Phased plan

## Phase 0: Scaffolding and documentation lock

Deliverables:

- `fork-docs/` structure and specs (this phase).
- Config key names and defaults finalized in docs.
- ADRs recorded.

Exit criteria:

- Documentation reviewed and accepted as source of truth.

## Phase 1: Storage foundation

Deliverables:

- New SQLite lifecycle store package.
- Schema initialization/migration bootstrap.
- CRUD methods for current state and transition history.

Verification:

- unit tests for schema bootstrap and read/write paths.
- restart persistence test.

## Phase 2: Transition engine

Deliverables:

- Cycle processor that ingests live snapshot + per-upstream success map.
- Relevant-upstream tracking per latest successful snapshot.
- Auto-inactive transitions with strict coverage checks.

Verification:

- tests for multi-upstream edge cases.
- tests for partial outage no-op behavior.
- tests for resolved reactivation on reappearance.

## Phase 3: Response merge into alerts API

Deliverables:

- Merge persisted `inactive` into existing `alerts.json` response path.
- Keep `resolved` hidden by default.
- Preserve sorting and filtering behavior.
- Include `resolved` only when explicitly requested by state filter (`@state=resolved`).

Verification:

- API response tests for mixed live+inactive data.
- snapshot tests for stable JSON shape.

## Phase 4: Manual resolve API

Deliverables:

- `POST /alerts/{id}/resolve` endpoint.
- allowed transition guard (`inactive -> resolved` only).
- nullable actor persistence (`NULL` when unavailable).

Verification:

- endpoint tests for success and conflict states.
- auth-on/auth-off behavior tests.

## Phase 5: UI integration

Deliverables:

- Add `inactive` and `resolved` to client state types.
- Add yellow style mapping for `inactive`.
- Add resolve menu action for `inactive`.
- extend `@state` autocomplete/validation.

Verification:

- component tests for action visibility and state rendering.
- flow test: inactive resolve causes default grid disappearance.

## Phase 6: Retention and vacuum jobs

Deliverables:

- TTL cleanup task.
- limit cleanup task using last state change age.
- periodic incremental vacuum task.

Verification:

- deterministic cleanup tests with seeded timestamps.
- stats endpoint reflects cleanup/vacuum timestamps.

## Phase 7: Operational endpoints and telemetry

Deliverables:

- `GET /alerts-history/stats` endpoint.
- DB size and record counters.
- housekeeping status timestamps.

Verification:

- endpoint contract tests.

## Phase 8: Hardening and merge-safety checks

Deliverables:

- Review touched files for upstream conflict surface.
- document integration points and rationale.
- add testscript coverage for fork lifecycle basics.

Verification:

- full test run (backend + frontend).
- manual exploratory run with sample upstream failures.

## 4. Rollout strategy

- Feature flag/storage enable switch if needed for safe rollout.
- Start with non-production environment and observe DB growth.
- Validate cleanup behavior before long retention windows.

## 6. Current progress snapshot

As of current fork stage:

- Phase 0 completed.
- Phase 1 completed.
- Phase 2 completed.
- Phase 3 completed.
- Phase 4 completed.
- Phase 5 mostly completed (state types, color mapping, resolve action, resolved quick filter, stats panel).
- Phase 6 completed (TTL -> limit cleanup + incremental vacuum scheduler).
- Phase 7 completed (stats endpoint + UI visibility in settings).
- Phase 8 pending (hardening, merge-safety review, broader integration coverage).

## 5. Risks and mitigations

- **Risk:** false inactive due to incomplete polling.
  - **Mitigation:** strict relevant-upstream success gating.
- **Risk:** schema drift and migration errors.
  - **Mitigation:** idempotent migration + startup checks.
- **Risk:** UI confusion with new states.
  - **Mitigation:** explicit color/state rules and filter defaults.
- **Risk:** DB growth.
  - **Mitigation:** TTL + cap + vacuum telemetry.

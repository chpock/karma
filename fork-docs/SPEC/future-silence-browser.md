# Future Silence Browser

Status: deferred and not reachable from the production UI.

## Purpose

The repository retains the old Karma component for browsing silence rules. It is
not part of the compact silence editor described by
`fork-docs/SPEC/silence-editor-redesign.md` and is not a user-visible feature in
the current preview.

This component browses Alertmanager silence rules. It does not browse past alerts,
alert lifecycle history, or historical versions of silence rules. Those are
different features and require separate specifications.

## Code that already exists

The retained source under `ui/src/Components/SilenceModal/Browser/` includes:

- loading existing silence rules from `silences.json`;
- text search, sort direction, pagination, and an expired-silence toggle;
- rendering active and expired rules through `ManagedSilence`;
- edit, recreate, and delete actions;
- selection and mass deletion;
- component tests for the isolated Browser behavior.

The compact editor also has a safe external edit/recreate entry contract:

- the backend selects one physical Alertmanager member;
- it reads the current rule directly from that member;
- the form receives that exact rule and an opaque revision token;
- submit reads the same rule again and rejects changed or deleted originals before
  mutation.

A future Browser must use this contract rather than filling the editor from list
data.

## Deliberately not implemented

Before this feature can be exposed, it needs an independently approved product and
interaction specification covering:

- whether it is a separate page, modal, or another entry point;
- whether it lists only current Alertmanager rules or also retained rule history;
- navigation between browsing and the compact editor;
- permissions and confirmation behavior for single and mass deletion;
- behavior for partial Alertmanager failures and inconsistent HA members;
- pagination, search, sorting, mobile layout, keyboard navigation, and focus;
- refresh behavior after create, edit, recreate, and delete;
- visual and end-to-end test fixtures for the approved design.

There is intentionally no hidden route, query parameter, feature flag, or tab that
enables the retained Browser in production.

## Known risks in the retained code

- `silences.json` is an aggregated list and can hide temporary differences between
  physical members of one Alertmanager HA cluster.
- A list item can become stale between rendering and an action. Edit/recreate must
  use the fresh-read revision-token contract; delete and mass-delete need their own
  explicitly designed concurrency contract.
- Partial upstream failures need a visible warning rather than an apparently
  complete list.
- The current Browser mixes list, selection, deletion, and editor-entry concerns in
  one component tree. A future design may choose different boundaries.
- Existing component tests prove retained code behavior, not approval of its UX or
  readiness for production exposure.

## Conditions for future activation

1. Approve a dedicated specification and POC.
2. Decide the current-rules versus historical-rules scope.
3. Define HA, stale-data, delete, and partial-failure contracts.
4. Integrate through a dedicated runtime entry point without adding a Browse tab to
   the compact editor.
5. Add approved browser, accessibility, and end-to-end coverage.

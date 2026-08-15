# Spec: UI Contracts

## 1. Default visibility

Default grid includes projected states:

- `active`
- `suppressed`
- `unprocessed`
- `inactive`

`resolved` remains hidden by default.

## 2. State rendering contract

- UI uses API `state` as projected state.
- `rawState` and `isLive` are additive dimensions for refined visuals.

Suppressed split:

- `state=suppressed` + `isLive=true` -> current suppressed style.
- `state=suppressed` + `isLive=false` -> `suppressed-past` style token.
- A non-live suppressed card renders known `suppressionCauses` through the same
  silence-rule renderer and action controls as a live rule. Edit/Recreate first
  verifies the rule with Alertmanager; a missing rule is refused rather than
  treated as current. Unresolved members have no complete snapshot, so they use
  the ordinary live-rule fallback: current data when available, otherwise ID only.
- For a complete retained snapshot, the initially visible Edit/Recreate/Delete
  controls may be derived from its saved time window with the browser clock. This
  avoids a remote request solely to expand a historical card. It is a display hint,
  not an authority boundary: the fresh Alertmanager status response determines the
  operation opened by Edit/Recreate, and Alertmanager accepts or rejects Delete.

## 3. Filter contract

- `@state` filters projected states.
- `@is_live=true|false` filters live-presence dimension.
- Contradictory combinations are valid and return empty sets.

## 4. Counter badges

Group header and swimlane show:

- unprocessed
- suppressed current (`suppressedBreakdown.current`)
- suppressed past (`suppressedBreakdown.past`)
- inactive
- active

`suppressedBreakdown` is backend-owned and always present in API payload.
UI must not reconstruct current/past split from raw alert list as a fallback.

Badge actions:

- suppressed current adds `@state=suppressed` + `@is_live=true`
- suppressed past adds `@state=suppressed` + `@is_live=false`

Alt+click still applies negative state filter only (without forced append filters).

## 5. Resolve action

- Resolve button is visible in alert menu.
- Current UI policy: enabled for `state=inactive`.
- Backend policy: resolve endpoint is authoritative and idempotent for non-live alerts.
- On success, UI forces next fetch and reflects backend `applied` result.

## 6. Warning and stats

- Persistent warning banner must be shown while `GET /alerts-history/warning` returns `dbExpectedState=false`.
- Configuration modal displays fork storage stats, including dynamic state counts and suppressed current/past split.

Malformed individual Alertmanager silence entries appear through the existing toast
channel as non-fatal warnings. Valid neighboring rules and alerts continue to load.

## 7. Compact silence editor

- All normal create/edit/recreate actions open the compact editor; no Browse tab or
  legacy editor route exists.
- One modal-level header owns the title and close action: `Create silence`,
  `Edit silence`, or `Recreate silence`.
- One complete matcher expression occupies one field. Preview/autocomplete are
  scoped to the selected logical target.
- Known validation/rejection preserves the form. Unknown mutation outcome disables
  retry until that modal closes.
- Backdrop does not close. Dirty close requires discard confirmation; all close paths
  are blocked during submission.
- Focus enters the first editor control after lazy content mounts, stays trapped in
  the modal, and returns to the invoking control on close.
- Mobile uses a full-screen single-scroll layout with sticky header/actions.

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

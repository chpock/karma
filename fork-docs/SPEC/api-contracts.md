# Spec: API Contracts

## 1. Scope

This spec defines fork-specific API additions.

Existing upstream endpoints remain unchanged unless explicitly documented here.

## 2. Resolve endpoint

## 2.1 Route

- `POST /alerts/{id}/resolve`

Where `{id}` is alert identity used by current core/UI behavior.

## 2.2 Request

Body (optional):

```json
{
  "comment": "optional operator note"
}
```

If authentication is enabled and a user exists in request context, that user is stored as `changed_by`.
If no user context exists, `changed_by` is stored as `NULL`.

## 2.3 Preconditions

Resolve is allowed only when current state is `inactive`.

If state is not `inactive`, return conflict.

## 2.4 Behavior

On success:

1. update `alerts_current.state` to `resolved`.
2. append transition row to `alert_state_history` with:
   - `old_state=inactive`
   - `new_state=resolved`
   - `change_source=manual`
   - nullable `changed_by`
3. return success payload.

## 2.5 Response

Success (200):

```json
{
  "status": "success",
  "alertId": "...",
  "oldState": "inactive",
  "newState": "resolved",
  "changedAt": "2026-08-03T10:11:12Z",
  "changedBy": null
}
```

Possible errors:

- `404` alert not found.
- `409` state transition not allowed from current state.
- `500` storage failure.

## 3. Storage stats endpoint

## 3.1 Route

- `GET /alerts-history/stats`

## 3.2 Response

Success (200):

```json
{
  "status": "ok",
  "storageEnabled": true,
  "sqlitePath": "./var/karma-alert-history.sqlite",
  "retentionTTLHours": 720,
  "maxAlerts": 100000,
  "currentRows": 1234,
  "historyRows": 9876,
  "fileSizeBytes": 12345678,
  "pageCount": 3020,
  "pageSizeBytes": 4096,
  "freelistCount": 16,
  "lastHousekeepingAt": "2026-08-04T09:00:00Z",
  "lastIncrementalVacuumAt": "2026-08-04T09:05:00Z"
}
```

Notes:

- when lifecycle storage is disabled, `storageEnabled=false` and counters are omitted/zero-valued.
- `last*At` fields are nullable and can be `null` before first housekeeping run.

## 4. Alerts response behavior changes

`POST /alerts.json` behavior in fork:

- default state set includes `inactive` records from persistent store.
- `resolved` records are excluded by default.
- `resolved` records are included only if requested via explicit state filter (`@state=resolved`).
- non-state filters alone do not cause resolved records to be merged.

## 5. Backward compatibility

- Existing clients expecting upstream behavior should continue working.
- New fields/states must be additive where possible.
- Versioning strategy for breaking API shape changes must be documented if introduced later.

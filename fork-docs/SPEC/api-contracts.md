# Spec: API Contracts

## 1. Scope

This document describes fork-specific API behavior and additive response fields.

## 2. Resolve endpoint

### 2.1 Route

- `POST /alerts/{id}/resolve`

### 2.2 Request

Optional body:

```json
{
  "comment": "optional operator note"
}
```

### 2.3 Preconditions

- Resolve is allowed only when alert is non-live (`is_live=false`).
- Live alerts return conflict.

### 2.4 Idempotency contract

- If non-live alert is not yet resolved: apply transition (`applied=true`).
- If non-live alert is already resolved: return `200` no-op (`applied=false`, `reason=already_resolved`).

### 2.5 Success response

```json
{
  "status": "success",
  "alertId": "...",
  "oldState": "inactive",
  "newState": "resolved",
  "applied": true,
  "changedAt": "2026-08-03T10:11:12Z",
  "changedBy": null
}
```

### 2.6 Errors

- `404` alert not found
- `409` resolve is not allowed for live alerts
- `500` storage/internal errors

## 3. Alerts response (`POST /alerts.json`) additions

Each alert includes:

- `state` (projected user-facing state)
- `rawState`
- `isLive`
- `isResolved`

Group/grid objects can include:

- `suppressedBreakdown.current`
- `suppressedBreakdown.past`

`stateCount.suppressed` remains total suppressed count (compatibility).

Resolved merge visibility:

- resolved alerts are excluded by default,
- included only when request filters include explicit `@state=resolved`.

## 4. Storage stats endpoint

### 4.1 Route

- `GET /alerts-history/stats`

### 4.2 Response additions

- `stateCounts` (dynamic map)
- `suppressedBreakdown` (`current`/`past`)
- regroup status/progress and expected-state fields
- DB size/page/freelist and housekeeping timestamps

## 5. Warning endpoint

- `GET /alerts-history/warning`

Returns persistent-warning status for DB expected-state contract:

- `dbExpectedState`
- `reason`
- regroup status/error/progress

UI must keep warning visible while `dbExpectedState=false`.

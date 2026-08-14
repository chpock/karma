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
- `suppressionCauses` (always an array)

`suppressionCauses` is populated only for a retained, non-live suppressed alert and
describes its latest effective immutable evidence. Each member includes logical
target, physical source, silence ID, `known|unresolved` status, and nullable exact
revision details (time window, Author, Comment, matchers). An unresolved member has
identity only; the API never invents payload details. Live Alertmanager state remains
in `alertmanager[].silencedBy` and is not mixed with historical evidence.

Group/grid objects always include:

- `suppressedBreakdown.current`
- `suppressedBreakdown.past`

Even when both values are zero, the object is present as:

```json
"suppressedBreakdown": { "current": 0, "past": 0 }
```

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
- silence-history TTL, HA-inconsistent targets, and recheck queue/job progress

## 5. Warning endpoint

- `GET /alerts-history/warning`

Returns persistent-warning status for DB expected-state contract:

- `dbExpectedState`
- `reason`
- regroup status/error/progress
- HA-inconsistent targets and failed recheck status/error

UI must keep warning visible while `dbExpectedState=false`.

## 6. Invalid silence entries in Alertmanager responses

The Alertmanager silence list is decoded as a syntactically valid JSON array of
independent raw entries. Each entry is validated before it enters Karma's model.

- An invalid entry is logged, exposed as a non-fatal upstream warning, and omitted.
- Valid neighboring entries continue processing and the source refresh remains
  successful.
- If a previously known rule becomes invalid, it is absent from this successful
  source snapshot; normal HA-confirmed absence semantics apply.
- Unknown additional fields and absent optional metadata are accepted.
- A malformed top-level array fails the complete refresh because entry boundaries
  cannot be trusted; the previous confirmed source set is retained.

Every upstream object in `alerts.json` always contains `warnings: []`. A warning has
`entryIndex`, safe `silenceID`/`status` when available, `field`, stable `reasonCode`,
and a safe human-readable `message`. Raw entries, invalid values, matchers, comments,
credentials, and response bodies are never included in warnings or logs.

## 7. Compact silence editor endpoints

All routes are under `/silence-editor/` and scoped to one logical target.

### 7.1 Autocomplete

- `POST /silence-editor/labels`
- `POST /silence-editor/values`

Both request bodies contain `target`, `term`, and `contextMatchers`; the values
request also contains `name`. `contextMatchers` contains only completed valid
matcher expressions other than the field being edited. The backend validates and
compiles it independently. Responses provide required `contextFingerprint`, exact
`total`, `truncated`, and target/context-scoped label/value counts. Prefix
characters are literal; `%` and `_` are not SQL wildcards.

### 7.2 Status verification

- `GET /silence-editor/status?target=...&id=...`

Karma selects one deterministic writable physical member, performs a fresh GET, and
returns the exact form payload, server-derived `isExpired`, selected `member`, and an
opaque `revisionToken`. Missing or invalid rules fail the operation. The periodic
silence cache is not an authority for this endpoint.

### 7.3 Preview

- `POST /silence-editor/preview`

The request carries target, structured matchers, and optional cursor/snapshot/limit.
The response contains exact totals, page metadata, opaque snapshot/cursor,
`excludedNoRawLabels`, lifecycle dimensions, and matched/missing-label evidence.

Preview accepts at most 100 matchers and 64 KiB of JSON. One in-memory result is
limited to 100,000 matched alerts. Expired/evicted snapshots return HTTP 409 and must
be rebuilt. Preview fails closed if lifecycle storage is unavailable; it does not
silently return only live alerts.

### 7.4 Submit

- `POST /silence-editor/submit`

The request carries target, explicit `create|edit|recreate`, strict ID/route/token
identity, and a structured Alertmanager payload. Karma validates matcher, time,
metadata, and ACL before any mutation.

- `proxy:true`: Karma makes exactly one Alertmanager POST.
- `proxy:false`: Karma returns a normalized direct descriptor and correlation ID;
  the browser makes exactly one POST.
- Edit/Recreate perform another fresh read from the same member. Route, state,
  identity, deletion, or revision drift returns HTTP 409 with
  `requiresReopen=true` before mutation.
- `outcomeUnknown=true` means a write may have committed; retry is disabled for the
  current modal.

Submit accepts at most 100 matchers. Submit and fresh status reads have no extra
Karma byte limit by default. A positive `silenceEditor.maxPayloadBytes` applies the
same limit to both.

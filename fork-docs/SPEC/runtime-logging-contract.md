# Runtime Logging Contract

Status: implemented.

## 1. Purpose

This contract defines application-log requirements for the fork's alert lifecycle,
silence awareness, and compact silence editor. It covers successful operations,
expected rejections, unusual conditions, failures, detailed debug diagnostics, and
data that must never be logged.

The SQLite `alert_events` table and the application log have different purposes:

- `alert_events` is durable domain history for one alert;
- the application log explains runtime execution, timing, routing, queue work,
  external calls, failures, and operator actions.

Writing one does not replace writing the other.

## 2. Terminology and Submission Paths

### 2.1 Silence rule action

A silence rule action is one of:

- `create`: create a new Alertmanager silence rule;
- `edit`: update an existing silence rule using its existing ID;
- `recreate`: create a new rule from an expired rule without reusing the expired
  rule's ID.

Code may call these actions "silence mutations". User-facing documentation and log
messages should prefer the explicit action name.

### 2.2 Server-routed submission (`proxy: true`)

The browser sends the validated command to Karma. Karma sends the HTTP POST to the
selected Alertmanager member and reads its response. Karma can therefore log the
attempt, member, HTTP result, duration, returned silence ID, and an unknown outcome.

### 2.3 Browser-routed submission (`proxy: false`)

Karma validates the command and returns a descriptor containing the selected public
Alertmanager endpoint and the backend-normalized payload. The browser then sends the
HTTP POST directly to Alertmanager.

Karma can prove only that it issued the descriptor. It cannot prove that the browser
sent the POST or what Alertmanager returned. Browser console messages are local and
non-durable; they are not a substitute for the server application log.

Consequently, complete server-side audit logging of silence rule actions is possible
only for server-routed submission.

Decision: browser-routed submission is retained. Karma writes
`direct_descriptor_issued` to the server application log. The browser writes the
subsequent `upstream_write_started` and exactly one terminal event to its developer
console using the same correlation ID. Browser records use `console.info` for start
and confirmed success, `console.warn` for a known rejection, and `console.error` for
an unknown outcome. This console record is best-effort and is not durable server
audit evidence.

## 3. Supported Levels

The application supports `debug`, `info`, `warning`, and `error` through
`log.level`.

| Level | Required meaning |
|---|---|
| `debug` | Detailed execution diagnostics: operation start/end, no-op paths, batch and cache details, counts, durations, routing decisions, and per-item decisions where useful. |
| `info` | A meaningful operation completed or a meaningful persistent condition changed. Normal unchanged polling must not produce repetitive `info` noise. |
| `warning` | An operation was rejected or data was malformed/incomplete, but Karma remained operational and did not have an unknown remote result. |
| `error` | An operation failed unexpectedly, persistent processing is blocked, durable state may be inconsistent, or a remote write may have happened but its outcome is unknown. |

HTTP 4xx is not automatically a warning and HTTP 5xx is not automatically an error.
The level follows the semantic outcome. For example, malformed preview input is
`debug`; an ACL rejection is `warning`; an Alertmanager POST with unknown outcome is
`error`.

## 4. Structured Event Shape

Every fork runtime log record must have:

- `component`: stable subsystem name;
- `event`: stable lower-snake-case event name;
- a human-readable message;
- identifiers and counts needed to correlate the operation;
- `duration` on completed external calls and non-trivial background work;
- `error` on failures.

Allowed component names:

- `alert_lifecycle`;
- `silence_reconciliation`;
- `silence_recheck`;
- `lifecycle_housekeeping`;
- `lifecycle_regroup`;
- `silence_editor`;
- `silence_preview`.

Common fields, when available:

- `alert_id`, `silence_id`, `job_id`, `target`, `member`, `source`;
- `operation` (`create|edit|recreate|resolve`);
- `operator` for authenticated operator identity;
- `mode` (`server_routed|browser_routed`);
- `http_status`, `reason_code`, `count`, `duration`;
- `request_id` if request correlation is available.

Event names and field meanings are an operator-facing compatibility contract. They
must not be changed casually or reused for a different outcome.

## 5. Data Safety

The following data must never be logged at any level:

- complete alert labels or annotations;
- complete matcher values or matcher expressions;
- silence comments;
- raw silence, alert, request, or response payloads;
- authorization headers, cookies, CORS credentials, or arbitrary HTTP headers;
- revision tokens;
- URI user information, passwords, or unsanitized upstream URLs;
- local-storage contents.

The following are allowed:

- alert and silence IDs;
- logical target and physical member names;
- authenticated operator identity;
- operator-supplied author only where its distinction from authenticated operator
  is explicit;
- state names, reason codes, HTTP status, counts, durations, and payload byte size;
- matcher count and label count, but not their values.

Errors from remote services must be sanitized before logging. An arbitrary response
body must not be logged verbatim because it may contain credentials or user data.

## 6. Alert Lifecycle Events

### 6.1 Startup and shutdown

| Event | Level | Required fields and behavior |
|---|---|---|
| `storage_initialized` | `info` | Database path, schema version, current-row count. |
| `storage_disabled` | `debug` | Explain that lifecycle hooks are no-ops. Emit once, not per request. |
| `storage_open_failed` | `error` | Sanitized path and error. |
| `storage_close_failed` | `error` | Error. |
| `background_workers_started` | `debug` | Housekeeping interval and batch sizes. |
| `background_workers_stopped` | `debug` | Normal shutdown completion. |

### 6.2 Poll cycle

| Event | Level | Required fields and behavior |
|---|---|---|
| `poll_started` | `debug` | Live alert count, source count, successful/failed source counts. |
| `poll_completed` | `info` when durable state changed; otherwise `debug` | Inserted, updated, inactive, reactivated, skipped-incomplete, transitions, queued jobs, duration. |
| `poll_failed` | `error` | Stage (`snapshot|begin|reconcile|alerts|commit` where known), error, duration. |
| `alert_transition` | `debug` | Alert ID, old/new projected state, old/new live/resolved/suppressed dimensions, reason code. Do not log labels. |
| `alert_absence_deferred` | `debug` | Alert ID and reason (`incomplete_source_coverage|invalid_stored_sources`). |
| `target_transition` | `debug` | Alert ID, target, old/new state, and reason code. |
| `target_retired` | `info` | Alert ID, target, and reason code. |

Repeated unchanged alerts must not produce per-alert messages. Per-transition debug
records are expected at `debug`, even for a large cycle; aggregate counts remain the
normal `info` view.

### 6.3 Manual resolve

| Event | Level | Required fields and behavior |
|---|---|---|
| `resolve_requested` | `debug` | Alert ID and operator; never log the comment. |
| `resolve_applied` | `info` | Alert ID, operator, old/new state. |
| `resolve_noop` | `debug` | Already resolved, alert ID, operator. |
| `resolve_rejected` | `warning` | Live-state conflict or not found, alert ID, operator, reason code. |
| `resolve_failed` | `error` | Internal/DB failure, alert ID, operator, error. |

## 7. Silence Reconciliation Events

| Event | Level | Required fields and behavior |
|---|---|---|
| `source_snapshot_received` | `debug` | Source, target, sync status, valid-rule count, malformed-rule count. |
| `source_sync_failed` | `warning` on transition; `debug` while unchanged | Source, target, sanitized error category. Preserve last confirmed rules. |
| `source_recovered` | `info` | Source, target, previous status. |
| `source_retired` | `info` | Source and target. |
| `invalid_rule_ignored` | `warning` | Source/member, entry index, silence ID/status when safely available, field, stable reason code. Never log the raw entry or invalid field value. |
| `rule_discovered` | `debug` | Source, target, silence ID, valid status. |
| `rule_revision_created` | `debug` | Target, silence ID, rule/revision IDs, revision reason; no payload. |
| `rule_episode_opened` | `debug` | Target, silence ID, episode ID/number. |
| `rule_episode_closed` | `info` | Target, silence ID, episode ID, `absent_confirmed`. |
| `ha_disagreement_detected` | `warning` on transition | Target and member count; no rule payload. |
| `ha_disagreement_unchanged` | `debug` | Target and member count. |
| `ha_converged` | `info` | Target and previous disagreement duration when known. |
| `recheck_job_queued` | `info` | Job ID, opaque context ID, candidate count, queue depth, reason code. |

An invalid individual silence entry is absent from the successful source snapshot.
The warning is logged once per poll occurrence. It must not create a revision,
current relation, matching decision, or recheck input. A syntactically malformed
top-level response is a source-level refresh failure rather than an individual-entry
warning.

## 8. Silence Recheck Events

| Event | Level | Required fields and behavior |
|---|---|---|
| `job_selected` | `debug` | Job ID, status, queue depth, batch size. |
| `job_started` | `info` | Job ID, candidate total, pinned-rule count, source count. |
| `job_resumed` | `info` | Job ID and prior processed count after restart. |
| `batch_completed` | `info` if any promotion occurred; otherwise `debug` | Job ID, processed, promoted, stale, missing labels, ineligible, incomplete sources, duration. |
| `candidate_skipped` | `debug` | Job ID, alert ID, stable skip reason; no labels. |
| `candidate_promoted` | `debug` | Job ID, alert ID, target if target-only, transition ID. |
| `job_completed` | `info` | Job ID, total counters, total duration. |
| `job_failed` | `error` on transition | Job ID, stage, preserved root error. |
| `failed_job_blocks_queue` | `warning` once, then `debug` while unchanged | Failed job ID and queue depth. Do not overwrite the persisted root error. |
| `queue_empty` | `debug` | Emitted at most once per worker invocation, not per candidate. |

## 9. Housekeeping and Regroup Events

### 9.1 Housekeeping

- `housekeeping_started` (`debug`): retention values and max-alert limit.
- `housekeeping_completed` (`info` when rows/pages were removed, otherwise
  `debug`): alert TTL deletions, max-limit deletions, silence-history deletions by
  category, vacuum pages, resulting row counts, duration.
- `housekeeping_failed` (`error`): stage and error.

### 9.2 Regroup

- `regroup_not_required` (`debug`): grouping fingerprint.
- `regroup_started` (`info`): target fingerprint and total rows.
- `regroup_resumed` (`info`): target fingerprint and persisted progress.
- `regroup_batch_completed` (`debug`): processed, updated, total, duration.
- `regroup_completed` (`info`): totals and duration.
- `regroup_cancelled` (`debug` during normal shutdown, otherwise `warning`).
- `regroup_failed` (`error`): target fingerprint, persisted progress, error.

## 10. Silence Editor Events

### 10.1 Preview and autocomplete

| Event | Level | Required fields and behavior |
|---|---|---|
| `preview_requested` | `debug` | Target, matcher count, page limit, cursor/snapshot presence. |
| `preview_built` | `debug` | Target, candidate count, matched count, excluded-no-label count, duration, snapshot ID. |
| `preview_page_served` | `debug` | Snapshot ID, returned/total, cache hit, duration. |
| `preview_rejected` | `debug` for invalid input; `warning` for capacity limit | Target, reason code, counts where available. |
| `preview_failed` | `error` | Stage and error; no labels/matchers. |
| `preview_snapshot_expired` | `debug` | Snapshot ID and age where known. |
| `preview_snapshot_evicted` | `debug` | Snapshot ID, record count, cache entries/records after eviction. |
| `autocomplete_completed` | `debug` | Target, kind (`labels|values`), result count, exact total, live-overlay count, duration. Do not log the search term or returned values. |
| `autocomplete_failed` | `error` | Target, kind, stage, error. |

### 10.2 Edit/recreate status verification

- `status_verification_started` (`debug`): target, member, silence ID.
- `status_verification_completed` (`debug`): target, member, silence ID, valid
  operation, duration. Never log the revision token.
- `status_verification_not_found` (`warning`): target, member, silence ID.
- `status_verification_failed` (`error`): target, member, silence ID, HTTP status,
  sanitized error, duration.

### 10.3 Create/edit/recreate

Every submission receives one correlation ID generated by Karma. All server-side
records for that user action carry it.

| Event | Level | Required fields and behavior |
|---|---|---|
| `submission_received` | `info` | Correlation ID, operation, target, selected member, mode, operator, author, matcher count. Do not log comment or matcher values. |
| `submission_validation_failed` | `debug` | Correlation ID, operation, target, stable reason code. |
| `submission_acl_rejected` | `warning` | Correlation ID, operation, target, member, operator, ACL rule index when known. |
| `submission_conflict` | `warning` | Correlation ID, operation, target, member, silence ID, reason (`route_changed|original_changed|original_missing|state_changed`). Never log the revision token. |
| `upstream_write_started` | `info` | Correlation ID, operation, target, member, silence ID if editing. Write immediately before the POST. |
| `upstream_write_succeeded` | `info` | Correlation ID, operation, target, member, returned silence ID, HTTP status, duration. |
| `upstream_write_rejected` | `warning` | Correlation ID, operation, target, member, HTTP status, sanitized reason, duration. Outcome is known. |
| `upstream_write_outcome_unknown` | `error` | Correlation ID, operation, target, member, HTTP status if known, failure stage (`request|read_body|decode_body|missing_id|timeout|aborted|upstream_5xx`), duration. Browser Fetch cannot distinguish DNS/connect/upload/CORS failures, so those use `request`. |
| `direct_descriptor_issued` | `info` | Correlation ID, operation, target, member, operator, `server_result_observable=false`. This is not a success record. |

The existing message "submitted silence" must be used only after a confirmed
server-routed success. Descriptor issuance must never be described as submission
success.

For browser-routed submission, the same event names and fields are emitted as a
structured object in the browser developer console. No descriptor payload, endpoint,
headers, matcher data, comment, author, or raw response body may be included.

## 11. HTTP Error Logging

- Expected malformed client input is returned to the client and logged at `debug`
  with a stable reason code, not the raw body.
- Authentication and authorization rejection is `warning` with operator/target but
  no credentials.
- A stale edit conflict is `warning`; it is not an internal server error.
- Store, serialization, and unexpected endpoint failures are `error`.
- Stats and warning endpoint failures must be logged before returning HTTP 500.
- Request access logging controlled by `log.requests` remains separate and does not
  satisfy these semantic-event requirements.

## 12. Noise and Deduplication

- Unchanged poll/housekeeping cycles are `debug`, not `info`.
- Persistent conditions such as HA disagreement, failed source, or failed recheck
  job emit `warning`/`error` when the condition first appears. Repeated observations
  are `debug` until recovery or a meaningful change.
- Recovery/convergence emits one `info` record.
- No polling loop may emit one `info`, `warning`, or `error` record per unchanged
  item indefinitely.
- Debug mode intentionally permits per-transition and per-skipped-candidate records.

## 13. Verification Requirements

Tests must capture structured log records and verify:

- required event name and level;
- required correlation/identity fields;
- success, no-op, rejection, error, and unknown-outcome paths;
- edge-triggered warning behavior and recovery records;
- direct descriptor issuance is not logged as successful action;
- comments, matcher values, labels, revision tokens, credentials, and raw bodies do
  not appear in serialized records;
- text and JSON log formats preserve the structured fields;
- filtering by `log.level` exposes detail only at the expected level.

Executable evidence:

- structured fields and level filtering: `internal/log/event_test.go`;
- lifecycle transition, silence reconciliation, source recovery, HA edge events,
  and redaction: `internal/fork/alertlifecycle/store_test.go`;
- Resolve levels and comment redaction:
  `cmd/karma/fork_alertlifecycle_resolve_test.go`;
- browser descriptor, ACL rejection, server unknown outcome, correlation, and
  payload redaction: `cmd/karma/silence_editor_test.go`;
- browser-routed start/success/rejection/unknown/abort events and callback
  isolation: `ui/src/Components/SilenceModal/SilenceEditor.test.tsx`;
- text/JSON handler behavior and configured level parsing: `internal/log/log_test.go`
  and `cmd/karma/main_test.go`.

The browser-routed terminal result remains deliberately best-effort and non-durable:
it exists only in the browser developer console because Karma does not observe that
POST. The correlated server `direct_descriptor_issued` event records the exact audit
boundary and must not be interpreted as operation success.

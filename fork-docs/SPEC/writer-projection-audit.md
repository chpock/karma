# Writer and Projection Inventory

Status: Problems 2 and 3 immutable input, retry, and operator recovery are implemented.
Problem 4 typed results, event ownership, strict APIs, and UI presentation are implemented
and independently accepted.

This inventory follows writes into every observable projection. It is organized
by domain transaction, not by source file.

## W1: Poll cycle

- Authority: one merged poll observation plus successful-member map and current
  configured source manifest.
- Transaction: one Store-owned `BEGIN IMMEDIATE` transaction after all network
  reads; the writer is acquired before lifecycle rows are read.
- Current rows: `alert_states`, `alert_target_states`, `alertmanager_sources`,
  `alertmanager_source_silences`, `alertmanager_cluster_consistency`.
- Append-only rows: `alert_events`, `alert_target_events`, silence episodes and
  revisions, live global/target cause histories and members.
- Materialized projections: `alert_target_match_labels`, grouping snapshots,
  source/target membership JSON and fingerprints.
- Jobs: promotion-capable evaluation changes enqueue a context and copy exact
  successful-source rule revisions.
- Readers: merged alert APIs (including effective suppression evidence), preview,
  autocomplete, status/warning/stats,
  recheck worker, housekeeping.
- Failure: any persistence failure rolls back the whole observation; failed
  network sources update captured health but cannot overwrite last-confirmed
  remote relations.
- Restart: all schema v1 domain state survives close/reopen.

## W2: Recheck worker

- Authority: oldest queued/running job's complete source context, pinned rules, and
  immutable candidate rows. Candidate observation tokens, labels, relevant sources,
  and target members are copied at creation and never refreshed from current rows.
- Transaction: each batch uses one Store-owned `BEGIN IMMEDIATE` transaction to
  update candidate progress, target/global CAS projection, events, and exact causes
  atomically. Writer ownership is released between batches.
- Current rows: `alert_target_states`, `alert_states`, job status/counters and
  `silence_recheck_job_candidates.processed_at`.
- Append-only rows: target/global transition events, target cause histories and
  aggregate links.
- Readers: alert APIs, preview, warning/stats, next worker invocation,
  housekeeping.
- Failure: first persisted error is immutable; failed predecessor blocks FIFO;
  stale CAS is a counted no-op; incomplete source context cannot promote. Each
  candidate receives exactly one constrained terminal outcome and parent counters
  satisfy the processed sum invariant. An
  unknown writer commit outcome is never persisted as a failed predecessor and
  never triggers a duplicate callback. Other transient/terminal worker error
  classification is typed: selected retryable BUSY-family failures use the bounded
  durable retry schedule, terminal failures block FIFO, and shutdown/acquisition failures
  do not consume attempts. Late candidate attachment is not supported.
- Restart: queued/running per-candidate progress resumes from the same immutable
  membership and pinned input. Eligibility under an unchanged source context creates
  a new targeted successor rather than mutating an older job. Retry scheduling and
  operator recovery use the same immutable job, append-only attempt history, and audited
  same-job operator actions.

## W3: Manual resolve

- Authority: manual request for a stored non-live alert.
- Transaction: one Store-owned `BEGIN IMMEDIATE` transaction commits global and every
  target dimension plus transition events atomically.
- Current rows: `alert_states`, `alert_target_states`.
- Append-only rows: global and target events; existing suppression evidence is
  not rewritten.
- Readers: every alert API and preview.
- Forbidden: resolve live; partial global-only resolve; stale recheck later
  overwrites resolution.

## W4: Housekeeping

- Authority: server clock, alert TTL/max count, reference-safe completed-job cleanup,
  and silence-history TTL.
- Transaction/order: Store writer ownership spans alert TTL, max-count, phase-2
  reference-safe retention, incremental vacuum, and vacuum metadata. The order is
  alert TTL, max-count, phase-2 retention, incremental vacuum.
- Deletes: alert rows through cascades, older completed unreferenced jobs and their
  job-owned graph on the next housekeeping cycle, retired sources, and unreferenced
  rule/episode/revision history after the silence-history TTL.
- Protected: current source relations, `queued`, `running`, `retry_wait`, `failed`, and
  `abandoned` jobs, cause-referenced completed jobs, the latest completed job as
  bounded post-run diagnostic context, and all job/evidence referenced history.
- Readers: stats and all lifecycle APIs.
- Restart: schema v1 FK actions remain identical after close/reopen; one blocked
  child must not roll back unrelated eligible deletion.

## W5: Regroup

- Authority: grouping config fingerprint and stored raw labels.
- Transaction: each bounded batch uses one Store-owned `BEGIN IMMEDIATE` transaction
  to update only receiver/group ID/group-label snapshot and regroup metadata.
- Forbidden: lifecycle dimensions, targets, evidence, raw match labels, source
  relations, or event history change.
- Readers: merged alert APIs and regroup warning.

## W6: Silence status

- Authority: request-time fresh GET from one deterministic writable member plus the
  server clock.
- Output only: exact member payload, `isExpired`, member route token, and opaque
  revision token; no mutation.
- Failure: no writable member, missing/invalid silence, transport error, oversized
  configured payload, or malformed request.
- Consumer: editor replacement generation; stale/aborted responses cannot fill or
  open the form.

## W7: Silence submit

- Authority: mutation identity tuple, selected-member request-time snapshot,
  canonical matcher compiler, server clock, ACL.
- Validation order: request/body limits, operation/ID/route/member state,
  matcher/time/metadata normalization, ACL, then exactly one mutation path.
- Proxy: one backend POST. Direct: one normalized descriptor followed by one
  browser POST with 30-second deadline.
- Caches/projections: successful or ambiguous response forces immediate refresh;
  endpoint does not write lifecycle SQLite directly.
- Failure: known rejection returns editable state; route/state/content drift
  requires reopen; ambiguous result remains terminal for the current modal only.

## W8: Editor replacement/session/success

- Authority: `SilenceEditorSession` plus one store-owned replacement generation
  and AbortController; submit has its own mounted/generation guard.
- Current RAM state: operation, route member, original ID, revision token,
  normalized draft,
  dirty/preflight/progress/result.
- Side effects: successful submit persists author, requests refresh, and closes
  exactly once.
- Forbidden: create carries prior route/ID/comment; stale async completion changes
  a replacement editor; submitting/unknown outcome allows another mutation.
- Readers: modal shell, compact editor, external edit/recreate entry, close guard.

## Projection Checklist Result

| Writer | Current | History | Index | Cache | FK/retention | Restart | Async/concurrency | Readers listed |
|---|---|---|---|---|---|---|---|---|
| W1 poll | yes | yes | yes | API purge required | cascade products | schema v1 reopen tests | writer mutex | yes |
| W2 recheck | yes | yes | n/a | request reads current | retained job/rule products | persisted candidate/rule resume | FIFO/CAS | yes |
| W3 resolve | yes | yes | target labels unchanged | API refresh path | cascade reviewed | persisted | CAS conflict tests | yes |
| W4 housekeeping | delete | preserve/delete | cascade | stats next read | latest/referenced retention products | schema v1 reopen tests | writer mutex | yes |
| W5 regroup | grouping only | metadata | n/a | API purge/warning | no domain delete | resumable | background batches | yes |
| W6 status | none | none | none | no cache; fresh member GET | n/a | n/a | generation/abort | yes |
| W7 submit | upstream only | next poll | next poll | force refresh | n/a | n/a | at-most-once | yes |
| W8 session | RAM | local author only | n/a | preview RAM | n/a | reset on open | generation/abort | yes |

Every row above has executable boundary coverage listed in
`architecture-verification.md` and the referenced test suites.

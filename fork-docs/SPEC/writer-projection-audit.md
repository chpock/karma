# Writer and Projection Inventory

Status: implemented reference.

This inventory follows writes into every observable projection. It is organized
by domain transaction, not by source file.

## W1: Poll cycle

- Authority: one merged poll observation plus successful-member map and current
  configured source manifest.
- Transaction: one `ApplyPollCycle` SQL transaction after all network reads.
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

- Authority: oldest non-completed job's immutable source context, target-qualified
  pinned rules, and immutable candidate membership.
- Transaction: each batch updates candidate progress, target/global CAS projection,
  events, and exact causes atomically.
- Current rows: `alert_target_states`, `alert_states`, job status/counters and
  `silence_recheck_job_candidates.processed_at`.
- Append-only rows: target/global transition events, target cause histories and
  aggregate links.
- Readers: alert APIs, preview, warning/stats, next worker invocation,
  housekeeping.
- Failure: first persisted error is immutable; failed predecessor blocks FIFO;
  stale CAS is a counted no-op; incomplete source context cannot promote.
- Restart: queued/running per-candidate progress resumes; eligibility transitions
  attach specific alerts without a mutable-state keyset gap; rule/status context
  never reads mutable current relations.

## W3: Manual resolve

- Authority: manual request for a stored non-live alert.
- Transaction: global and every target dimension plus transition events commit
  atomically.
- Current rows: `alert_states`, `alert_target_states`.
- Append-only rows: global and target events; existing suppression evidence is
  not rewritten.
- Readers: every alert API and preview.
- Forbidden: resolve live; partial global-only resolve; stale recheck later
  overwrites resolution.

## W4: Housekeeping

- Authority: server clock, alert TTL/max count, silence-history TTL.
- Order: alert TTL, max-count, phase-2 reference-safe retention, incremental
  vacuum.
- Deletes: alert rows through cascades, unreferenced old completed jobs/rule pins,
  retired sources and unreferenced rule/episode/revision history.
- Protected: current source relations, non-completed/latest jobs, and all job/
  evidence referenced history.
- Readers: stats and all lifecycle APIs.
- Restart: schema v1 FK actions remain identical after close/reopen; one blocked
  child must not roll back unrelated eligible deletion.

## W5: Regroup

- Authority: grouping config fingerprint and stored raw labels.
- Transaction: bounded batches update only receiver/group ID/group-label snapshot
  and regroup metadata.
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

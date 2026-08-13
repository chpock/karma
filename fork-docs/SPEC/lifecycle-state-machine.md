# Lifecycle State Machine

## Authority

- `raw_state` comes only from a successful Alertmanager observation.
- `is_live` means present in the latest complete relevant-source observation.
- `is_suppressed` is sticky evidence and clears only on live reappearance.
- `is_resolved` is manual-only and clears only on live reappearance.
- projected `state` is derived by `LifecycleDimensions.ProjectedState()`; SQL writers
  do not define an independent state machine.
- configured source manifest is scoped by logical target; failed/unknown coverage
  preserves that target's last-confirmed member set, while removed or reassigned
  members retire the old target without fabricating a remote state change.

## Pure transitions

| Event | Preconditions | Result |
|---|---|---|
| live(raw) | successful observation | `is_live=1`, raw replaced, resolved cleared, suppression equals raw |
| absent | complete relevant coverage | `is_live=0`, suppression/resolution sticky |
| resolve | non-live, unresolved | `is_resolved=1` |
| silence promotion | non-live, inactive, unresolved, unsuppressed | `is_suppressed=1` |
| target retirement | previous target has no configured members | retain dimensions, set `retired_at`, remove active indexes |
| target reappearance | live target observation | clear `retired_at`, apply live(raw) |

The pure engine lives in `internal/fork/alertlifecycle/state_machine.go`. Store code
computes a transition first, then atomically persists current rows, append-only
events, and causes in one transaction. Global and target transitions use the same
dimension functions.

## Suppression evidence

- Live target evidence records known/partial/none member resolution without creating
  synthetic silence revisions.
- Recheck target evidence links one target event to one pinned job and exact revision
  set.
- Sticky targets use their latest immutable history; they are never re-matched by a
  later job.
- A global transition after multiple target promotions links all effective target
  history IDs in `alert_target_aggregate_suppression_*`.
- Silence revision identity compares the latest episode revision's normalized
  content and nullable UTC upstream `updatedAt`. Any change appends the next
  revision; returning to an older content/timestamp pair never reuses old history.
- An open silence episode closes with `absent_confirmed` only when every configured,
  non-retired HA member of that logical target has completed a successful snapshot
  and no current source relation remains. Partial failure never closes an episode;
  later reappearance starts the next episode number.

## Failure and observability

- Recheck jobs are strict FIFO. A failed predecessor persists `last_error` and blocks
  later jobs.
- Stats expose queue depth, current ID/status, progress, skipped count, oldest age,
  and error.
- HA inconsistency is last-confirmed state: failed coverage cannot clear it; complete
  successful convergence can. Silence identity is part of the member fingerprint;
  equal payloads with different Alertmanager IDs remain inconsistent because
  edit/delete identity differs.
- Recheck enqueue compares previous and next evaluation state. Only new active,
  pending/expired-to-active, matcher/time evaluation changes, recovery, convergence,
  or topology reassignment/retirement can enqueue; deletion, expiration,
  `updatedAt`-only and display-only changes cannot.
- Source context fingerprints include each rule's evaluation fingerprint. A matcher
  change therefore queues a distinct successor job even while an older context is
  still queued/running; each job retains its own pinned revision set.
- Recheck job context captures every source status, but pinned rules are copied only
  from sources that were successful at job creation. A relevant failed, invalid, or
  unknown source makes that candidate incomplete; stale last-successful relations
  can never satisfy the job. Recovery creates a complete successor context.
- Retiring a source clears only its mutable `alertmanager_source_silences` current
  relations. Immutable job/evidence references remain. Expired completed-job rule
  pins are pruned after silence-history TTL so unreferenced source/rule/revision
  history can be collected.

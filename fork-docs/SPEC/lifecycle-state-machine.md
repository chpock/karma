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

- Recheck jobs are strict FIFO with explicit `queued`, `running`, `retry_wait`, `failed`,
  `completed`, and `abandoned` states. A failed or not-yet-due retry predecessor preserves
  immutable first-error fields plus latest error fields and blocks later jobs. Five
  typed retry waits are persisted; the sixth consecutive retryable failure is terminal.
  Each immutable candidate reaches one constrained terminal outcome; parent counters
  equal processed membership.
- Selected attempts are append-only and tokenized. Pre-selection acquisition failures and
  cancellation-first shutdown create no attempt. A selected retryable or terminal operation
  error established before later cancellation remains the durable failure cause and is persisted
  under a bounded internal bookkeeping context. Commit uncertainty is reconciled by
  the attempt token before another candidate batch can start; transient tuple reads keep
  reconciliation pending, while only durable absence or mismatch is invariant. Internal
  recovery deadlines are not shutdown. A connection-invalidating reconciliation read
  replaces the dedicated writer without losing pending state, and generic-token absence
  requires attempt-token absence. Selection, committed batches, failure, Retry, and
  Abandon all persist the result of the same pure job-event reducer. Failed jobs advance
  only through an audited same-job operator retry or explicit abandonment; both preserve
  immutable input and progress.
- The single scheduler wakes only on startup, committed job creation, committed operator
  action, persisted due time, and a delayed cross-operation reconciliation of a selected
  `recheck_batch` attempt. Immediate and reconciled committed `recheck_failure` use one
  finalizer for retained selection/terminal events; only a terminal outcome publishes and
  releases a terminal-failure finalization barrier. Reconciled failure resumes through the active reconciliation timer
  and retained retry/failed result without a finalizer wake. A separate operator-action
  finalization barrier keeps recheck execution behind the applied event attempt without
  changing timer ownership.
  Ordinary or immediately reconciled empty/no-work commits never wake it; housekeeping is
  not a poller. A durable failed head keeps the owner alive for Retry or Abandon, while a
  terminal infrastructure disposition exits the scheduler owner until application restart.
- Stats expose the nested `recheck` object with queue depth and the nullable FIFO
  `headJob`. The head includes durable status, progress/outcome counters, attempt and
  retry counters, timestamps, and complete first/last error snapshots. Completed and
  abandoned historical rows are not exposed as the current head.
- HA inconsistency is last-confirmed state: failed coverage cannot clear it; complete
  successful convergence can. Silence identity is part of the member fingerprint;
  equal payloads with different Alertmanager IDs remain inconsistent because
  edit/delete identity differs.
- Recheck enqueue compares previous and next evaluation state. Source-context jobs are
  created after the complete poll reconciliation; an eligibility transition under an
  unchanged context creates one targeted successor. Only new active,
  pending/expired-to-active, matcher/time evaluation changes, recovery, convergence,
  or topology reassignment/retirement can enqueue; deletion, expiration,
  `updatedAt`-only and display-only changes cannot.
- Source context fingerprints include each rule's evaluation fingerprint. A matcher
  change therefore queues a distinct successor job even while an older context is
  still queued/running; each job retains its own pinned revision set.
- Recheck job context captures every source status, but pinned rules are copied only
  from sources that were successful at job creation. Each candidate also pins global /
  target observation tokens, raw labels, relevant-source references, and target source
  members. A relevant failed, invalid, or unknown source makes that candidate
  incomplete; stale last-successful relations can never satisfy the job. Recovery
  creates a complete successor context.
 - Retiring a source clears only its mutable `alertmanager_source_silences` current
  relations. Immutable job/evidence references remain. `queued`, `running`,
  `retry_wait`, `failed`, and `abandoned` jobs are retained; older completed
  unreferenced job-owned rule pins are deleted with their job during the next
  housekeeping cycle. Unreferenced retired sources are collected by `retired_at`,
  unreferenced revisions and rules by `last_observed_at`, and closed episodes by
  `ended_at` under the silence-history TTL.

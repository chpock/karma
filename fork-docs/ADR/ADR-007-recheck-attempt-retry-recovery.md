# ADR-007: Recheck Attempt Retry and Operator Recovery

- Status: Accepted
- Date: 2026-08-19

Implementation status: Implemented and independently accepted, including
selected-error/shutdown precedence and the complete scheduler state/event matrix.
The combined fresh
schema-v1 shape was accepted by ADR-006; this ADR defines the runtime ownership and
durability rules for the reserved attempt and operator-action tables.

## Context

Problem 2 creates an immutable, FIFO-ordered recheck job and pins every value needed for
its candidate decisions. Before Problem 3, the worker could convert an infrastructure error
after job selection into a permanent `failed` job without an attempt ledger or bounded
retry policy. A failed head then blocked every newer job, while a process restart could not
distinguish a committed batch from a batch whose client-side commit result was lost.

This decision adds recovery without changing the immutable job input or creating a second
queue owner. Normal shutdown cancellation does not consume retry budget, and an operator
can make an explicit, audited decision about a failed head.

## Decision

### Durable state machine

The job status is one of `queued`, `running`, `retry_wait`, `failed`, `completed`, or
`abandoned`. FIFO selection considers the oldest job in any non-terminal state. A
not-yet-due `retry_wait` and a `failed` head block later jobs; only committed progress,
operator retry, or operator abandonment can release the head.

The five automatic retry delays are `1s`, `5s`, `30s`, `2m`, and `10m`. The sixth
consecutive retryable selected-attempt failure becomes terminal `failed`. A committed
batch resets only `consecutive_retry_count`; total `retry_count` and first-error history
are retained.

Only `queued` may have a null job `started_at`. A committed selection persists it in the
batch transaction; if that transaction rolls back, durable failure bookkeeping restores
the same immutable attempt start time before writing `retry_wait` or `failed`.

Pre-selection writer acquisition failures and cancellation-first shutdown are infrastructure
outcomes outside the job state machine: they create no attempt row, do not increment counters,
and do not persist a job failure. If a selected retryable or terminal operation error is already
established and cancellation starts afterward, that selected error owns classification and durable
failure bookkeeping runs under a bounded internal context. Unknown Commit remains higher precedence
than both selected-error and cancellation classification.

### Attempt ledger and commit ownership

An opaque attempt token and the next attempt number are generated only after durable job
selection, after verifying `attempt_count = COUNT(attempts)` for the job. A selected batch
inserts its `committed` attempt row in the same transaction as candidate outcomes and
parent progress. A rolled-back retryable or terminal attempt is recorded in a separate
writer transaction after the batch transaction releases writer ownership. The process-local
attempt identity retains the pre-attempt total/consecutive retry counters so idempotent
failure-bookkeeping replay never reinterprets a fifth scheduled retry as a sixth failure.
Every commit
confirmation validates the complete durable tuple `(attempt_token, job_id,
attempt_number, expected_outcome)`; a mismatch or missing row is a terminal writer
invariant. The attempt token makes failure bookkeeping idempotent.

The Problem 1 writer ledger still proves that the writer transaction committed, while
the Problem 3 attempt tuple proves which job outcome became durable. Commit errors retain
their original cause until present/absent reconciliation; an absent token is classified
through the same BUSY allowlist as any other storage error. A reconciliation query error
leaves the outcome unknown, keeps both ledgers pending, and blocks another candidate
batch. It is retried without caching the transient result. Only durable absence or a
tuple mismatch is a writer invariant. Internal recovery deadlines are never interpreted
as worker cancellation unless the worker's own context is canceled. Transactions and
failed connections are never reused after a statement or commit error.

A connection-invalidating generic-token or attempt-tuple lookup error discards the
retained logical connection and acquires/configures a replacement without clearing the
pending writer operation, attempt, original cause, finalizer, or handoff. Generic writer
token absence is accepted only after the same finalizer confirms attempt-token absence;
generic absence plus any durable attempt tuple is an invariant violation.

Failure classification is typed. Post-selection retryable SQLite codes are exactly
`5`, `261`, `517`, and `773`. Other storage errors, invariant failures, schema errors,
and corrupt pinned input are terminal. Acquisition origin is separate from acquisition
retryability: typed bad/closed connections and allowlisted acquisition codes use
acquisition backoff. `ErrWriterUnavailable` records replacement state but is not itself a
retry class; unknown or non-allowlisted typed acquisition causes terminate the scheduler
even when joined with that marker. Failure messages are
sanitized operational text; labels, matchers, comments, headers, and raw payloads never
enter attempt rows or logs.

### Single scheduler owner

`OpenStore` remains passive. `cmd/karma` owns exactly one scheduler goroutine, started
after Store initialization and joined before Store close. A capacity-one wake channel and
a timer for the persisted head due time coordinate startup recovery, committed creation,
operator actions, and retry waits. Confirmed poll job-creation and operator commits use an
explicit ordinary handoff owned by the original post-commit finalizer; any later writer may
reconcile it, but the original handoff runs exactly once. A recheck batch already owned by
the active scheduler does not hand off after an ordinary or immediately reconciled commit;
its retained finalizer uses a separate reconciled-wake handoff only when a delayed,
cross-operation reconciliation confirms a real selected attempt. Empty/no-work commits
never use that handoff.

The scheduler retains one timer state with kind (`process_backoff` or
`persisted_head_due`), owner, and absolute deadline. A capacity-one wake received while
that timer is active is coalesced but cannot run the worker early; the timer event is the
only trigger that clears the timed state. Process-local acquisition, pre-attempt,
failure-bookkeeping, commit-reconciliation, and queue-read failures have independent
`1s`, `5s`, then capped `30s` backoff state. Named success boundaries reset only their
completed owners: pre-attempt success resets acquisition/pre-attempt, reconciliation
success resets reconciliation plus pre-attempt, bookkeeping success resets runner owners,
and queue-read resets only after a successful head read. Commit uncertainty resets
pre-attempt and advances only reconciliation. Recovered bad/closed writer connections
remain recoverable acquisition outcomes. Unclassified invariant errors terminate the
scheduler owner until application restart. Housekeeping is not a periodic scheduler wake
source. A durable terminally failed head is a normal wakeable FIFO disposition: the owner
stays alive, emits `job_failed` exactly once after confirmation, and waits for Retry or
Abandon rather than terminating or creating a replacement job. Terminal bookkeeping
infrastructure errors terminate the owner. Writer acquisition failures before the
failure-bookkeeping callback starts retain the pending attempt and use the independent
acquisition backoff; only typed retryable bookkeeping commit/storage failures use the
bookkeeping backoff. A callback error remains terminal even when rollback/replacement
cleanup also reports writer unavailability. Reconciled `recheck_failure` operations reset
acquisition, pre-attempt, bookkeeping, and reconciliation owners according to the same
provenance table as unknown outcomes. The scheduler releases Store writer ownership
between batches and never runs parallel poll-triggered or housekeeping-triggered workers.

Process-local backoff is not persisted as job retry state. A pending bookkeeping or
commit-reconciliation record blocks a second candidate attempt until its durable outcome
is confirmed or shutdown begins.

### Operator actions

Authenticated routes are:

- `POST /alerts-history/recheck/{jobID}/retry`
- `POST /alerts-history/recheck/{jobID}/abandon`

Both actions are semantic CAS operations on the same job row and append an action row in
the same transaction. Retry is only `failed -> queued`, preserves immutable input,
progress, and attempts, clears the active schedule, and increments recovery count.
Abandon is only `failed -> abandoned`, requires a non-empty reason, records actor/time,
and releases the FIFO head without deleting any input or history.

Request identity is `(jobID, requestId)`. The canonical fingerprint is the versioned
typed object `{action, normalizedReason, normalizedActor}`. An exact replay returns the
original result; a different action, reason, or actor for the same request ID returns
HTTP 409. An exact replay creates no second operator-action row, job/attempt/candidate
change, domain event, or scheduler wake. Because the replay is itself a committed Store
writer invocation, it appends exactly one operational `lifecycle_writer_commits` row; that
row is not operator domain state and does not violate semantic idempotency. Request bodies
are strict, case-sensitive JSON objects: duplicate keys,
case-variant keys, non-string/null values, invalid UTF-8, trailing values, unknown fields,
and bodies over 4096 bytes are rejected. Reasons are never emitted in runtime logs.

## Consequences

- Retry and recovery survive restart because schedule, counters, errors, attempts, and
  operator actions are durable.
- A transient selected-attempt failure cannot permanently poison the queue before its
  retry budget is exhausted.
- Commit uncertainty cannot duplicate candidate outcomes, promotions, events, or
  suppression evidence.
- Operators explicitly control terminal failed jobs without losing FIFO position or
  immutable decision input.
- Problem 4 owns the observability/UI presentation boundary without changing this
  retry, job-state, candidate, or provenance contract.

## Verification

Problem 3 requires production-boundary state/retry matrices, durable attempt and restart
tests, typed failure-boundary tests, full attempt-token reconciliation tests,
deterministic scheduler owner/timer tests, strict application-router authentication and
operator CAS tests, and behavioral schema tests. The implementation is covered by the
capability matrix in `../SPEC/architecture-verification.md`.

Final-closure verification additionally covers every scheduler event-emission branch under
a panicking logger, exact operator request-ID rejection before Store access, and the complete
six-state/seven-event reducer matrix. A logging failure is isolated after durable control
state is established; it cannot alter a persisted retry timer, backoff owner, or terminal
decision.

The scheduler's fallback `observability_collection_failed` event remains a registered
`warning` event. Both its primary and fallback emissions are panic-isolated, and a real
Store retry-scheduling run compares durable retry state against the non-panicking control.

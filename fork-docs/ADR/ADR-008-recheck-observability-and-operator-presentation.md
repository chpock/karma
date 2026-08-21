# ADR-008: Recheck Observability and Operator Presentation

- Status: Accepted
- Date: 2026-08-19

Implementation status: Implemented and independently verified on 2026-08-21.

## Context

Problems 1-3 established the durable lifecycle writer, immutable recheck input,
and the retry/recovery state machine. The remaining presentation layer still
collapses independent conditions into one warning string, exposes an incomplete
flat recheck snapshot, and lets worker, Store, and command wrappers emit
overlapping events.

## Decision

Problem 4 presents the state already established by the lifecycle Store and
recheck scheduler. It does not add job states, retry policy, candidate
membership, or provenance behavior.

The recheck worker returns a typed result with a stable disposition and only
metadata confirmed by committed state. The command scheduler owns invocation,
retry, blocker, and terminal infrastructure events. The Store owns durable
domain events and transaction-local poll events after commit. An edge-sensitive
in-memory marker suppresses repeated blocker warnings without becoming domain truth.

The non-durable `infrastructure_failed` disposition represents a selected
invocation whose transaction outcome is terminal infrastructure failure. It is not a
job status and is emitted by the scheduler as the single `job_infrastructure_blocked`
event. Pre-selection writer acquisition remains `acquisition_failed` and uses only
`worker_acquisition_failed`.

`queue_blocked` and `retry_deferred` are successful typed results with no execution
error. Their metadata comes from the durable queue-head read. Other operational errors
retain the Problem 3 error contract. `retry_scheduled` keeps its operational error for
the retry owner but its warning event is emitted only by the scheduler after durable
failure bookkeeping; Store-owned `job_selected` precedes it. `job_failed` exists only for
a terminal selected-failure path and is never part of retry scheduling. It is emitted only after the durable failure
transaction commits and carries the durable status, counters, stage, class, and a safe
error category.

Immediate and delayed committed retry/terminal bookkeeping share one recheck-specific event
finalizer. A terminal `failed` outcome publishes a terminal-failure finalization barrier
under writer ownership, then emits the retained selection/terminal events and releases the
barrier after unlock. Retry and Abandon wait for that barrier outside `writeMu`. A committed operator mutation publishes a
separate operator-action finalization barrier; recheck execution checks it before and
inside writer ownership so `operator_action_applied` is attempted before operator-released
work is selected. Neither barrier changes generic writer ownership or Problem 3 timer/
backoff policy.

`/alerts-history/stats` exposes the complete nonterminal FIFO head under
`recheck.queueDepth` and nullable `recheck.headJob`; it does not return a
completed or abandoned historical row as the head. `/alerts-history/warning`
returns exactly `status`, `storageEnabled`, `dbExpectedState`, and an ordered
`issues` array. Issue metadata is a strict discriminated union for regroup,
silence HA inconsistency, and recheck retry/blocker states.

Operator Retry and Abandon remain the Problem 3 authenticated semantic-CAS
routes. The UI renders every simultaneous issue, shows controls only for a
failed head and an authorized session, confirms abandonment, prevents duplicate
requests, and refreshes from the server after every result including HTTP 409.

Observability collection is best effort. Failures in counters, `RowsAffected`,
event metadata, or post-commit logging never alter committed domain state,
queue decisions, retry scheduling, or operator results.

Recheck completion retains the operation-specific typed result and event selection payload
before Commit. Durable reconciliation validates the writer and attempt ledgers but does not
perform a new fallible result read. Immediate and delayed terminal failure share one
post-unlock finalization barrier: Retry and Abandon check it before writer acquisition and
inside their transaction, wait without holding the writer right, and preserve semantic CAS.
A separate operator-action barrier keeps recheck execution behind the best-effort
`operator_action_applied` attempt and does not change scheduler timer ownership.

## Rejected alternatives

- Keeping the scalar warning response would overwrite simultaneous independent
  failures and preserve ambiguous UI semantics.
- Returning the latest completed job as the current head would misrepresent
  FIFO liveness and create stale operator controls.
- Letting each layer log its own failure would duplicate events and make edge
  behavior dependent on wrapper call paths.
- Deriving recheck status in the UI would bypass server-authoritative state and
  expose unsafe action controls.

## Verification

Problem 4 requires Store and command event-sequence tests, strict stats and
warning API matrices, observability-failure no-effects tests, registered-route
operator tests, and light/dark UI state tests. The final capability matrix is
recorded in `../SPEC/architecture-verification.md`.

The accepted matrix includes immediate and delayed writer reconciliation, terminal-failure
and operator-action finalization barriers, exact event ownership, strict API projections,
semantic operator replay with one operational writer-ledger row, request-generation races,
panic/no-effects recovery, and rendered color/contrast evidence. The bounded final Go, race,
UI, browser, build, and lint gate is recorded in that capability matrix.

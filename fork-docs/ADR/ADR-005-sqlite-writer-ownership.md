# ADR-005: SQLite Writer Ownership

- Status: Accepted
- Date: 2026-08-16

## Context

The lifecycle Store currently serializes known writers with `Store.writeMu`, but
runtime write transactions are opened through the shared `database/sql` pool as
deferred transactions. A recheck transaction can therefore read its queue state
and fail when its first write attempts to upgrade that read transaction while
another SQLite connection owns the WAL writer lock. The observed error was
`SQLITE_BUSY` while changing a queued job to running. The later failed-job write
then persisted the transient error and blocked the FIFO queue.

The deployment scope is one Karma process with one local SQLite database. WAL
read concurrency remains useful for preview, autocomplete, merge, stats, and
warning queries. Reducing the whole Store to one SQLite connection is therefore
not an acceptable solution.

## Decision

All lifecycle mutations use one Store-owned writer connection and one Store-level
writer ownership boundary:

1. `Store.writeMu` serializes lifecycle writer ownership.
2. The owner uses the Store's retained `*sql.Conn` for runtime mutations.
3. The transaction starts in immediate write mode before the operation performs
   any reads (`BEGIN IMMEDIATE`, provided by modernc SQLite's
   `_txlock=immediate` setting).
4. The operation commits exactly once on success and rolls back on every error or
   panic path.
5. Writer ownership and the writer connection remain held until transaction
   completion.
6. The shared `Store.db` pool remains available for read-only operations and is
   not restricted to one connection.

The writer connection is acquired during `OpenStore`, retained for the Store
lifetime, and closed before the read pool in `Store.Close`. If transaction
completion leaves its driver connection state uncertain, the connection is
discarded and replaced under `writeMu`; persistent database setup PRAGMAs are not
re-executed during replacement.

The shared DSN contains only connection-local settings and `_txlock=immediate`.
`journal_mode(WAL)` and `auto_vacuum(INCREMENTAL)` are applied and verified once
through the owned writer connection before schema bootstrap and before read-pool
traffic begins. No read-pool connection may execute those write-capable setup
PRAGMAs.

Every lifecycle mutation, including schema bootstrap, poll, recheck, resolve,
housekeeping/vacuum, regroup, metadata writes, and failure bookkeeping, must use
the same writer boundary. Long-running regroup and recheck operations still split
their work into separate transactions and release ownership between batches.

Writer helpers must not call themselves recursively. A failure after a recheck
job has been selected must return from the batch transaction before failure state
is persisted through a separate writer-helper invocation. Retry, recovery, and
terminal-failure classification remain the contracts of Problems 2 and 3.

## Rejected Alternatives

### Deferred transactions with only `busy_timeout`

Rejected because the failure occurred during a deferred read-to-write upgrade and
the busy handler is not a sufficient guarantee at that stage. A timeout can remain
a defensive setting, but it is not the ownership contract.

### `SetMaxOpenConns(1)` and `SetMaxIdleConns(1)`

Rejected because it serializes read-only operations, removes WAL read concurrency,
and hides rather than fixes writer ownership defects.

### A second mutex or per-method transaction policy

Rejected because it would create multiple ownership authorities and allow future
write paths to bypass the actual SQLite writer connection.

### Retries or backoff in Problem 1

Rejected for this phase. Retry and transient/terminal classification belong to
Problem 3 and must not mask an unidentified transaction-boundary defect.

## Consequences

- A lifecycle writer reserves SQLite write mode before reading mutable state.
- The deferred read-to-write upgrade race is removed from the runtime writer path.
- Read-only Store operations retain multi-connection WAL concurrency.
- Writer transactions may wait for the SQLite writer lock before reading and hold
  it for the complete operation.
- All lifecycle write paths must be audited and routed through the helper.
- Problem 1 adds no domain tables or migration behavior. The Step 0
  `schema_layout_fingerprint` metadata guard is part of fresh v1 bootstrap and
  rejects unknown physical layouts without modification.
- Problems 2 and 3 have their own schema-v1 changes and ADRs.

## Verification

Problem 1 must provide deterministic tests for:

- modernc SQLite error codes `5` and `517` in their distinct deferred-transaction
  scenarios;
- a normal WAL reader while an immediate writer is open;
- no `job_selected` before writer acquisition succeeds;
- queued-job preservation when acquisition fails;
- worker progress after a competing writer releases the lock;
- immediate transaction startup on the configured Store;
- every lifecycle mutation using the writer boundary;
- commit/rollback behavior, panic cleanup, connection replacement, and close
  ordering.

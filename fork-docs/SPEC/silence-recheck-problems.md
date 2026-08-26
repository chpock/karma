# Silence Recheck Problems to Resolve

This document records the confirmed root causes from the silence recheck incident and
the approved execution plan. Implement the problems in the stated order. For each
problem, add a regression that fails for the documented reason before changing its
production behavior, then complete that problem's code, tests, and documentation
gate before starting the next one. Problems 2 and 3 share one approved final
schema design but remain separate test-first implementation phases.

## Confirmed Facts

- The lifecycle SQLite database is used by one Karma process and one container.
- The process has multiple `database/sql` connections, which is normal.
- A POSIX `READ` lock on the database and `-shm` files is normal in SQLite WAL mode;
  it does not by itself identify a blocked writer.
- Job 547 first failed while changing from `queued` to `running` with `SQLITE_BUSY`.
- The worker persisted that error and changed the job status to `failed`.
- Later log entries report the persisted failed predecessor; they do not prove that
  SQLite is returning a new `SQLITE_BUSY` on every poll.
- Job 541 grew from 17 to 19 candidates while it was running. This contradicts the
  documented immutable-candidate contract.

## Operational Preconditions and Step 0

The database inspected during the incident is diagnostic and disposable. There are
no deployed lifecycle databases whose contents must survive these schema changes.
Before running a binary that contains the Problem 2 or Problem 3 schema, delete the
old diagnostic database file together with its `-wal` and `-shm` sidecars while
Karma is stopped. Never delete database files from application startup code.

Problem 1 verification uses a fail-fast PID test harness. No testscript helper may wait
only for PID-file existence. The managed helper observes readiness, child exit, and its
own startup deadline, terminates/reaps its owned process, and reports early startup
failure in seconds. Auxiliary Alertmanager readiness must be confirmed before the Karma
PID wait starts. Do not increase the package timeout as a substitute. This is test
infrastructure, not a lifecycle domain feature.

Keep the logical schema version at v1. Add metadata key
`schema_layout_fingerprint` and a compile-time expected SHA-256 value. Compute the
actual fingerprint from sorted application-owned `sqlite_schema` rows represented as
typed JSON records containing `type`, `name`, `tbl_name`, and non-null `sql`. Exclude
objects whose names start with `sqlite_`; include every application table, explicit
index, and trigger. Do not normalize away SQL tokens or constraints.

For an empty database, apply the complete DDL in one immediate transaction, compute
the actual fingerprint, require it to equal the compile-time expected value, then
write `schema_version` and `schema_layout_fingerprint` in that transaction. For a
non-empty database, compute the actual fingerprint and read both metadata values
before executing any DDL. Require actual, stored, and compile-time fingerprints to be
identical.

Startup must reject an older v1 layout that lacks the fingerprint or has different
tables, columns, constraints, or indexes.
It must return a clear error instructing the operator to stop Karma and recreate the
disposable database. It must not silently run `CREATE TABLE IF NOT EXISTS` against an
incompatible v1 layout, auto-delete the database, migrate it, or partially repair it.

Step 0 was applied before implementing Problem 1: currently false conformance claims were marked
`Known violation; tracked by silence-recheck-problems.md`. Problems 1-10 and the associated
conformance decisions are implemented and independently accepted; all owning focused,
integration, documentation, and completion gates passed.
The operator
replay-ledger contract is normative in ADR-005, ADR-007, `storage-schema.md`, and
`implementation-contracts.md`.

Record the approved architecture in fork ADRs before the corresponding broad code
change:

- `fork-docs/ADR/ADR-005-sqlite-writer-ownership.md` before Problem 1;
- `fork-docs/ADR/ADR-006-immutable-recheck-job-input.md`, including the combined
  final Problems 2-3 DDL, before Problem 2; and
- `fork-docs/ADR/ADR-007-recheck-attempt-retry-recovery.md` before Problem 3.
- `fork-docs/ADR/ADR-008-recheck-observability-and-operator-presentation.md` before
  Problem 4's observability and UI changes.

## Resolution Order

### 1. Establish the Root Cause of the Original SQLite Busy Error

**Status: Problems 1-10 implemented and independently accepted.**

The writer-ownership implementation, fail-fast PID test infrastructure, typed
acquisition and cancellation handling, domain-state regressions, writer/shutdown
evidence, and final verification are complete for Problem 1. Problems 2 and 3 are
implemented.
Problems 4, 5, and 9 passed their final focused/integration gates and are independently
accepted.
Problem 8 is implemented and independently accepted. Problem 6 is implemented and
independently accepted. Problems 7 and 10 have no remaining problem-specific findings and
passed their final integrated documentation gates.

The remediation includes the durable `lifecycle_writer_commits` ledger, typed
`committed`/`not_committed`/`unknown` outcomes, bounded internal recovery contexts,
single-owner rollback, replacement retry, commit-unknown mutation blocking, a
30-day ledger retention rule, writer-entrypoint barriers, shutdown joins before storage
close, real-phase/event-identity evidence, and schema-fingerprint coverage for views.

**Exact observed symptoms**

1. Job 547 was queued and selected by the recheck worker.
2. The worker entered `RunSilenceRecheckBatchContext`, opened a deferred SQLite
   transaction, read the job, and then failed on the first write that should have
   changed the job from `queued` to `running`.
3. The original failure was logged as:

   ```text
   failed to mark recheck job running:
   database is locked (5) (SQLITE_BUSY)
   ```

4. Job 547 did not reach `job_started` and did not process candidates in that failed
   attempt. The transaction was rolled back.
5. The deferred error handler then successfully wrote `status='failed'` and the
   original error into `silence_recheck_jobs`.
6. Every later poll selected job 547 as the oldest failed predecessor and stopped
   before attempting to run it. The repeated message was effectively:

   ```text
   silence recheck job 547 is failed and blocks later jobs:
   failed to mark recheck job running: database is locked (5) (SQLITE_BUSY)
   ```

   This is the persisted historical error, not proof of a new `SQLITE_BUSY` on every
   poll.
7. The deployment was checked with `lsof`, `fuser`, `lslocks`, `ps`, and Docker
   inspection. Only one Karma process in one container used the database, WAL, and
   shared-memory files.
8. `lsof` showed multiple SQLite file descriptors belonging to that same process.
   This is expected for a `database/sql` pool and does not identify the conflicting
   operation.
9. `lslocks` showed POSIX `READ` locks. With SQLite WAL enabled, those locks are not
   evidence of a permanently held writer lock.
10. The Store contract serializes its known lifecycle writers with `Store.writeMu`,
    and the SQLite DSN uses WAL mode with a five-second busy timeout. Despite those
    protections, the one-time transition to `running` returned `SQLITE_BUSY`.

**Established diagnosis**

- `modernc.org/sqlite` enables SQLite extended result codes. The observed
  `database is locked (5) (SQLITE_BUSY)` was therefore primary code `5`, not
  `SQLITE_BUSY_SNAPSHOT` code `517` hidden behind the same primary class.
- In this exact sequence, code `5` means that another SQLite connection still held
  the WAL writer lock when the recheck transaction attempted to upgrade from its
  read transaction to a write transaction. A writer that had already committed and
  invalidated the recheck read snapshot would have produced code `517` instead.
- The five-second busy timeout does not guarantee a wait during a deferred
  read-to-write upgrade. SQLite may return code `5` immediately without invoking the
  busy handler at that stage.
- A complete audit of the current production code found one production `Store` and
  no lifecycle write path that bypasses `Store.writeMu`. It also found no leaked
  production transaction or result set. The identity of the historical lock holder
  cannot be recovered from the later process and file-lock inspection.
- An ordinary WAL reader cannot explain this failure by itself. The required
  conflicting condition was an active writer on another SQLite connection.

**Required solution**

Replace independently managed runtime write transactions with one Store-owned writer
boundary. That boundary must reserve SQLite write mode with `BEGIN IMMEDIATE` before
the operation performs any reads. Keep the normal multi-connection read pool so WAL
read concurrency is preserved.

This is the only implementation authorized for this problem. Follow the steps below
in order. Do not start Problem 2 or Problem 3 while implementing this plan.

The shared `database/sql` DSN must contain only per-connection settings that do not
mutate database layout: `synchronous(NORMAL)`, `foreign_keys(ON)`,
`busy_timeout(5000)`, and `_txlock=immediate`. Remove `journal_mode(WAL)` and
`auto_vacuum(INCREMENTAL)` from the shared DSN. Execute those two write-capable
PRAGMAs once through the owned writer connection before schema bootstrap and before
read-pool traffic begins. Verify their returned values. A read-pool connection must
never execute a write-capable setup PRAGMA outside the writer boundary.

#### Step 1: Add deterministic regression coverage before changing Store behavior

Add focused tests under `internal/fork/alertlifecycle`. Use a temporary SQLite file,
at least two explicit SQLite connections, and synchronization barriers. Do not use
`time.Sleep` as the mechanism that decides when a transaction may proceed.

Introduce a package-private transaction-start abstraction used by the writer helper.
Its production implementation must delegate directly to `*sql.Conn.BeginTx`. Tests
may provide a barrier implementation from `_test.go` to observe that transaction
acquisition has started. Do not expose this abstraction through `StoreOptions`,
configuration, build output, or any public API. This is the only permitted mechanism
for the lock-order barrier; arbitrary production callbacks and timing hooks remain
forbidden.

The tests must establish all of the following separately:

1. A deferred transaction that has completed a `SELECT` receives modernc error code
   `5` when another connection still owns the WAL writer lock and the deferred
   transaction attempts its first `UPDATE`.
2. A deferred transaction receives code `517` when another connection commits after
   the deferred transaction's `SELECT` but before its first `UPDATE`. This test keeps
   the two failure mechanisms from being conflated again.
3. A normal WAL reader continues to read while another connection owns an immediate
   write transaction.
4. The recheck worker cannot emit `job_selected` while another connection holds the
   writer lock. Use a synchronized test log handler or an equivalent test-only
   observation boundary; do not add sleeps or production-only timing hooks.
5. After the competing writer releases the lock, the worker starts the queued job and
   does not persist a lock-related `failed` state.
6. If transaction acquisition exits before obtaining the writer lock, the worker has
   not selected or modified a job. The original job must remain queued with its rules,
   candidates, counters, and error fields unchanged.

Inspect SQLite errors with `errors.As` and `(*sqlite.Error).Code()`. Do not classify
errors by matching their text.

#### Step 2: Give the Store one explicit runtime writer connection

Keep `Store.db` as the multi-connection pool used by read-only methods. Add one
Store-owned `*sql.Conn` obtained from that pool for runtime writes. The writer
connection must be acquired during `OpenStore`, normally retained for the Store
lifetime, and closed before `Store.db` in `Store.Close`.

The writer connection is replaceable under `writeMu`. If transaction completion
leaves connection state uncertain, mark its underlying driver connection bad via
`sql.Conn.Raw` returning `driver.ErrBadConn`, close the `*sql.Conn`, and obtain a fresh
one from the pool. Verify only connection-local DSN settings on the replacement.
`journal_mode(WAL)` and `auto_vacuum(INCREMENTAL)` are persistent database setup and
must not be executed again during replacement. Reconcile durable state before
allowing another write attempt. No caller may retain the old connection after
replacement.

Add `_txlock=immediate` to the modernc SQLite DSN. The driver must therefore execute
`BEGIN IMMEDIATE` for every non-read-only `BeginTx` on the writer connection. Add a
test that proves the configured Store starts a write transaction in immediate mode;
do not rely only on the DSN string being present.

Do not call `SetMaxOpenConns(1)` or `SetMaxIdleConns(1)` on the Store database. That
would serialize readers and remove the WAL read concurrency this design must retain.

#### Step 3: Introduce one writer ownership helper

Add one Store-level helper that:

1. acquires `Store.writeMu`;
2. gives the operation exclusive access to the Store writer connection;
3. starts a non-read-only transaction before the operation performs any SQL reads;
4. executes the supplied operation;
5. commits exactly once on success;
6. rolls back on every error or panic path; and
7. releases `Store.writeMu` only after the transaction has finished.

Use a second internal helper only where one writer ownership interval must contain
more than one transaction or a write-capable PRAGMA, as in housekeeping. The outer
helper must still own `writeMu` and the writer connection for the complete interval.
Document clearly that writer helpers must never call themselves recursively, because
recursive acquisition would deadlock.

Preserve wrapped SQLite errors with `%w` so later code and tests can inspect the
modernc error code. Do not add retries, backoff, or error-string parsing in this
problem.

#### Step 4: Move every lifecycle mutation to the writer boundary

Audit and convert every current production mutation, including:

- schema bootstrap;
- `UpsertCurrentAlert`;
- `InsertStateTransition`;
- `RunHousekeepingContext`, `PRAGMA incremental_vacuum`, and its metadata update;
- `ResolveAlertContext`;
- `ApplyPollCycle`;
- `beginOrResumeRegroup`, `runRegroupBatch`, and `failRegroup`;
- `RunSilenceRecheckBatchContext`, including failed-job persistence; and
- every direct `Store.db.Exec`, `Store.db.ExecContext`, `Store.db.Begin`, or
  `Store.db.BeginTx` that can mutate lifecycle storage.

`UpsertCurrentAlert` currently performs its alert upsert and candidate attachment as
two independent autocommit statements. Put both statements in the same writer
transaction so the operation becomes atomic.

For recheck, acquire the immediate transaction before `selectNextRecheckJobTx`. If
transaction acquisition fails, `selectedJobID` must still be zero and failed-job
persistence must not run. Keep the existing behavior for domain errors that occur
after a job has been selected; changing terminal failure and retry semantics belongs
to Problems 2 and 3.

Do not persist a selected job failure from inside a callback that still owns the
writer helper. First return from the batch writer callback so its transaction is
rolled back and writer ownership is released. Only then persist the existing failed
state through a new writer-helper invocation. Calling the writer helper recursively
would deadlock.

Keep regroup and recheck batch boundaries unchanged. They must continue releasing
writer ownership between batches so poll, resolve, and housekeeping can make
progress.

Keep the exact housekeeping order unchanged: alert TTL cleanup, max-alert
enforcement, phase-2 retention, transaction commit, incremental vacuum,
last-vacuum metadata, then stats collection. Do not move vacuum ahead of cleanup,
limit enforcement, or phase-2 retention.

#### Step 5: Prove that no runtime write bypass remains

After the conversion, perform a production-code audit for all `Begin`, `BeginTx`,
`Exec`, `ExecContext`, and write-capable PRAGMA calls in
`internal/fork/alertlifecycle`.

The final inventory must show:

- read-only methods use the multi-connection pool;
- runtime mutations use only the Store writer connection and writer helper;
- no runtime mutation writes through the read pool directly;
- every transaction has an explicit commit-or-rollback path; and
- no `Rows` value can outlive its transaction or return path.

Add or expand a serialization test that fails if any public writer can proceed while
the writer helper is deliberately held by another operation. Do not treat a source
code search alone as regression coverage.

Use a table-driven writer matrix covering schema bootstrap, alert upsert, transition
insert, poll, resolve, housekeeping/vacuum, every regroup phase, every recheck phase,
failure bookkeeping, and metadata writes. Also test rollback on callback error and
panic, exactly one commit on success, partial `OpenStore` cleanup, uncertain writer
connection replacement, and writer-before-pool close order. Verify a newly opened
read-pool connection cannot execute write-capable initialization PRAGMAs.

#### Step 6: Update the implemented architecture records

Only after the code and regression tests pass, update the SQLite writer contract and
writer inventory in:

- `fork-docs/ARCHITECTURE.md`;
- `fork-docs/SPEC/implementation-contracts.md`;
- `fork-docs/SPEC/architecture-conformance.md`;
- `fork-docs/SPEC/architecture-verification.md`; and
- `fork-docs/SPEC/writer-projection-audit.md`.

Record that runtime writers use one Store-owned writer connection with immediate
transactions while readers retain the WAL multi-connection pool. Do not describe the
new boundary as implemented before its production path and regression tests exist.

**Explicit non-goals and prohibitions**

- Do not add a database migration or compatibility path. There are no deployed
  lifecycle databases to migrate; modify the schema-v1 bootstrap directly only if a
  schema definition must change. This problem is not expected to require a schema
  change.
- Do not add a new job status, retry counter behavior, retry schedule, or backoff.
- Do not recover, delete, requeue, or otherwise modify an already persisted failed
  job. That is Problem 2.
- Do not classify transient and terminal worker errors. That is Problem 3.
- Do not retry an `UPDATE` inside a failed transaction.
- Do not increase the busy timeout as the solution.
- Do not reduce the entire Store to one database connection.
- Do not serialize read-only preview, autocomplete, merge, stats, or warning queries
  behind the writer mutex.
- Do not add a second independent writer mutex or allow individual public methods to
  choose their own transaction mode.
- Do not add production test hooks, timing sleeps, or fallback write paths.
- Do not leave write-capable connection-initialization PRAGMAs in the shared DSN.

**Acceptance criteria**

- The worker reserves SQLite write mode before reading or selecting a recheck job.
- A competing writer can no longer cause a deferred read-to-write failure after job
  selection.
- Failure to acquire the writer transaction leaves every persisted job column and
  every pinned rule and candidate row unchanged and does not create a failed
  predecessor.
- Releasing a short-lived competing writer allows the same worker invocation to start
  and process the queued job through the existing busy-timeout behavior.
- Ordinary WAL readers continue while the immediate writer transaction is open.
- All production lifecycle mutations pass the single writer ownership boundary.
- Read-only Store operations retain multi-connection concurrency.
- No queue recovery, retry policy, or later-problem behavior is introduced.
- Focused lifecycle tests, sequential bounded Go tests (`-timeout 180s`), root `make lint`, and
  `git diff --check` all pass.

### 2. Create Immutable, Provenance-Complete Recheck Jobs

**Status: implemented**

**Problem**

The pre-remediation implementation did not have one atomic job-creation boundary. A job
was identified only by its source context, while candidates could be attached later by
the worker, by an alert transition, or by `UpsertCurrentAlert`. This mixed historical
rules with alerts from a later lifecycle episode and made trigger and counter data
incorrect. The current implementation creates the immutable parent, rules, candidates,
and provenance in one creator transaction; the remediation is independently accepted.

Immutable membership alone was not sufficient. Before remediation, a candidate row
stored only `job_id`, `alert_id`, and `processed_at`; the worker read fresh labels,
source membership, target rows, and `last_seen_at` when it processed the candidate. An
old job could therefore evaluate a newer alert episode and use that newer value as its
CAS expectation. The current worker consumes the pinned input and validates exact
source/cluster completeness and provenance.

**Implemented contract**

- One transaction creates the job parent, pinned rules, complete candidate input,
  trigger provenance, and initial counters.
- Job rules and candidate rows never change after creation except for terminal
  processing outcome fields on an existing candidate row.
- A candidate pins the values needed to reproduce its decision and reject a newer
  lifecycle episode. It is not merely an `alert_id` membership marker.
- A source-context change may create one full-snapshot job after the poll has applied
  every alert and target transition.
- An alert or target that becomes eligible while the source context is unchanged
  creates a new targeted successor job. It is never appended to an older job.
- Jobs remain strict FIFO by `job_id`. A successor always receives a new `job_id`.
- Duplicate execution of the same poll transaction or enqueue request is idempotent
  and returns the already-created job instead of creating duplicate work.
- Job creation consumes a private canonical copy of poll input. Canonically sort
  targets, relevant source/upstream references, source members, suppression source
  references, silence IDs, silences, and matcher collections before fingerprinting or
  candidate creation. Never mutate caller-owned slices or maps.
- Two semantically identical poll snapshots with different input ordering create the
  same job fingerprint, pinned rules, candidates, and successor identity.

The combined final schema design covering every Problem 2 and Problem 3 table, column,
constraint, and index was recorded in the schema specification and ADR before the
implementation. Problem 2 is responsible for the job/rule/candidate portion; Problem 3
adds retry, attempt, scheduler, and operator-action behavior on the reserved v1 shape.
Both edit the fresh schema-v1 bootstrap directly; this is not a migration.

#### Step 1: Failing regressions and implementation evidence

Add the tests before modifying the production creator. The old implementation must
fail these tests for the expected reason; a test that already passes without the
production correction is not sufficient.

Add focused tests under `internal/fork/alertlifecycle` for these sequences:

1. Create a job with two candidates, run one batch, make another alert inactive, and
   verify that the original job still has exactly two candidate rows and its original
   `total_candidates`.
2. Make an alert inactive under an unchanged source context and verify that a new
   successor job is created with the new alert as its only work item.
3. Apply semantically identical poll inputs with permuted nested targets, sources,
   silence IDs, silences, and matchers in fresh identical Stores. Verify identical job
   fingerprints/rules/candidates and byte-identical caller input before/after.
4. Keep an older job open, change the source evaluation context, and verify that the
   new context creates a separate successor with its own pinned rules.
5. Create a candidate for lifecycle episode A, re-fire the alert, make it inactive as
   episode B, and verify that the episode-A candidate is a stale terminal outcome and
   cannot suppress episode B.
6. Repeat the previous test for one target row. The expected target observation token
   must be pinned independently from the global alert observation token.
7. Delete or evict the current alert row while a job still owns a candidate snapshot.
   Verify that job membership does not shrink and the missing current row becomes a
   terminal candidate outcome.
8. Verify that every candidate row reaches exactly one terminal outcome and that a
   completed job has `processed_candidates == total_candidates`.
9. Verify that source-triggered and alert-eligibility-triggered creations return the
   correct trigger, actual `created` result, and exact candidate count.
10. Re-run the same creator input and verify that it returns the same job without
   adding rules, candidates, or counters a second time.
11. Close and reopen the Store between batches and compare decoded domain fields for
    equality. Separately assert that canonical source/job/candidate JSON bytes and
    fingerprints are unchanged.

Replace `TestRecheckCandidateBehindCursorIsProcessedAfterBecomingInactive`. Do not
keep its current expectation that a late candidate is appended to an older job. The
replacement must expect an immutable predecessor and a new successor.

#### Step 2: Replace the baseline schema-v1 job identity

There are no deployed lifecycle databases. Modify the schema-v1 bootstrap directly.
Do not add `ALTER TABLE`, schema migration code, backfill, dual-read logic, or legacy
column fallbacks.

Replace source-context-only open identity with these separate concepts:

- `source_context_fingerprint`: identity of the immutable pinned source status and
  rule context;
- `job_fingerprint`: idempotency identity of the complete job input;
- `trigger_type`: constrained to `source_context_changed` or
  `alert_became_eligible`.

`job_fingerprint` must be a unique SHA-256 fingerprint over deterministic typed JSON
with a format version. Its input must contain:

- the complete canonical source context, not only its fingerprint;
- trigger type;
- every sorted pinned rule field used by evaluation, including source, target, stable
  silence identity, content/evaluation fingerprints, observed state, and evaluation
  fields; local database rule/revision IDs are foreign-key linkage only and are not
  fingerprint identity; and
- every sorted immutable candidate field, including alert ID, target scope, pinned
  global/target observation tokens, exact labels, relevant-source snapshot, and
  target-source members.

Do not use creation time, database row IDs, map iteration order, or batch order in
the fingerprint. Do not retain `UNIQUE(open_context_fingerprint)` as job identity;
multiple open jobs with the same source context are valid when their trigger or
candidate observation token differs.

Change `silence_recheck_job_candidates` so one row represents one immutable decision
scope. Use `(job_id, alert_id, target_key)` as its logical identity, where an empty
`target_key` means a global alert without a non-retired target row. Store at least:

- `expected_alert_last_seen_at`;
- nullable `expected_target_last_seen_at`;
- exact `match_labels_json` used for evaluation;
- exact relevant-source snapshot;
- nullable exact target-source-member snapshot;
- `processed_at`;
- nullable terminal `outcome`.

Define `target_key` as `TEXT NOT NULL`. Empty string is the only global-scope value.
For an empty target key, target-source-member snapshot fields must be NULL. For a
non-empty target key, the target-source-member snapshot must be present. A target
`last_seen_at` may itself legitimately be NULL, so do not use timestamp nullability
to distinguish global and target scope. Remove foreign keys from candidates to both
mutable global and target current-state tables.

Constrain `outcome` to exactly these stable machine values:

- `promoted`;
- `stale_alert_observation`;
- `stale_target_observation`;
- `missing_observation_token`;
- `ineligible_state`;
- `missing_current_alert`;
- `missing_current_target`;
- `missing_labels`;
- `invalid_labels`;
- `no_matching_rule`;
- `incomplete_source_coverage`;
- `retired_target`;
- `already_suppressed`; and
- `already_resolved`.

Rename `promoted_alerts` to `promoted_candidates`, because one candidate can represent
one target rather than one whole alert. Keep these aggregate parent counters and map
every terminal outcome to exactly one of them:

| Parent counter | Candidate outcomes |
|---|---|
| `promoted_candidates` | `promoted` |
| `stale_conflicts` | `stale_alert_observation`, `stale_target_observation` |
| `skipped_ineligible` | `missing_observation_token`, `ineligible_state`, `missing_current_alert`, `missing_current_target`, `no_matching_rule`, `retired_target`, `already_suppressed`, `already_resolved` |
| `skipped_missing_labels` | `missing_labels`, `invalid_labels` |
| `skipped_incomplete_sources` | `incomplete_source_coverage` |

Enforce in code and tests:

```text
processed_candidates = promoted_candidates
                     + stale_conflicts
                     + skipped_ineligible
                     + skipped_missing_labels
                     + skipped_incomplete_sources
```

Add golden canonicalization and SHA-256 vectors that cover reordered rules and
candidates, nil versus empty values, empty global target keys, non-empty target keys,
and RFC3339Nano UTC timestamp encoding.

The observation token is not a generic version column. It is the pinned
`last_seen_at` value together with the semantic state predicates already used by the
CAS. A normal poll-created inactive global or target row must have a non-null
`last_seen_at`. If a compatibility/nonstandard row lacks that value, retain the
candidate for immutable accounting but classify it as `missing_observation_token`;
never promote it through an `IS NULL` CAS branch. Do not add generic monotonically
increasing global or target version fields.

Do not use `ON DELETE CASCADE` from candidate rows to mutable `alert_states`. A
candidate snapshot must survive alert retention or max-alert eviction. The worker
must classify an absent current row as a terminal outcome. Update housekeeping tests
to prove that immutable open-job input is retained.

Keep job-rule rows immutable and continue referencing exact silence revisions. Add
schema tests for every new column, CHECK constraint, unique key, index, and foreign
key behavior; checking only that the table exists is insufficient.

Add an old-layout fixture with metadata `schema_version=1` but without the new
schema-layout fingerprint. `OpenStore` must reject it without modifying any table or
metadata row. Add a fresh-layout reopen test that validates the stored fingerprint
and succeeds. The error must identify an incompatible disposable lifecycle schema
without attempting migration.

#### Step 3: Introduce one transactional job creator

Create one internal input type and one creator function. The input must already
contain the final trigger, source context, pinned rules, and complete candidate
descriptors. The creator must return a result with:

- `job_id`;
- `created`;
- `trigger_type`;
- `candidate_count`; and
- `source_context_fingerprint` and `job_fingerprint` for internal correlation.

The creator is the only function allowed to insert into:

- `silence_recheck_jobs`;
- `silence_recheck_job_rules`; and
- `silence_recheck_job_candidates`.

Use `INSERT ... ON CONFLICT DO NOTHING RETURNING job_id` or an equivalent reliable
insert/query sequence. Do not use `LastInsertId` to detect a conflict. Do not make
logging depend on `RowsAffected`; logging metadata collection is best-effort and must
not roll back the domain transaction.

If `created=false`, load and verify the existing parent. A fingerprint collision with
different persisted input is a terminal invariant error. Compare canonical parent,
rule, and candidate input, not only parent columns. Do not append missing child rows
to make the existing job look compatible.

#### Step 4: Create jobs only after poll state reconciliation is complete

Refactor `ApplyPollCycle` so source reconciliation records whether a
promotion-capable source-context change occurred but does not create a job
immediately. While applying alert and target transitions, collect the exact
eligibility transitions created by that poll.

After all global and target state updates are complete, create jobs in this order:

1. If a promotion-capable source context changed, build one full candidate snapshot
   from the final state inside the same poll transaction and create one
   `source_context_changed` job. Do not also create alert-triggered jobs for
   candidates already covered by that full snapshot.
2. If the source context did not change, coalesce the exact alerts and targets that
   became eligible in this poll into one `alert_became_eligible` successor. Do not
   perform a full alert scan for this trigger.
3. If no candidate became eligible and there was no required source-context job,
   create no job.

Continue using the approved promotion-capable transition rules. New active rules,
pending/expired-to-active transitions, matcher/time evaluation changes, source
recovery, reassignment, and retirement may create a source-context successor.
Deletion, expiration, `updatedAt`-only changes, and display-only changes must not.

For alerts with non-retired target rows, create target-scoped candidate descriptors.
Create an empty-target global descriptor only for an eligible alert that has no
non-retired target row. Never let one target transition silently schedule all other
targets from the same alert.

#### Step 5: Remove every post-creation membership mutation

Delete `attachEligibleRecheckCandidatesTx` and remove its worker call. Replace
`enqueueAlertForCurrentRecheckContextTx` with transition collection plus the creator
described above. Remove candidate scheduling from `UpsertCurrentAlert`; that method
must be a single atomic compatibility upsert without scheduler side effects.

`ApplyPollCycle` is the only production authority allowed to create an eligibility
transition and schedule its successor. `UpsertCurrentAlert` is a compatibility and
test/bootstrap mutation API; production runtime code must not use it to represent a
poll transition. Add a production-call-site audit and a regression that proves this
method does not create a job.

After this step, a production-code search must find no INSERT into
`silence_recheck_job_candidates` outside the one creator. The worker may only update
terminal outcome fields on an existing row.

#### Step 6: Make worker processing membership-complete

Load pending candidate rows only by immutable job membership and terminal outcome,
not by current alert eligibility in the SQL `WHERE` clause. For each row:

1. load the current global/target state if it still exists;
2. compare current state and observation token to the pinned expected values;
3. evaluate only the pinned labels and pinned relevant-source membership;
4. apply the existing `last_seen_at` plus semantic-state CAS;
5. write transition/evidence if promotion succeeds;
6. write one terminal candidate outcome; and
7. update parent counters in the same transaction.

Classify in this exact priority order so one row can never receive different outcomes
depending on branch order:

1. missing current global alert;
2. missing required observation token;
3. stale global observation token;
4. missing current target for target-scoped work;
5. retired target;
6. stale target observation token;
7. already resolved;
8. already suppressed;
9. other ineligible semantic state;
10. missing or invalid pinned labels;
11. incomplete pinned source coverage;
12. no matching pinned rule;
13. promotion CAS; and
14. promoted.

If the final CAS affects zero rows, re-read the global/target semantic state in the
same transaction and classify it with the same priority as stale observation or
ineligible state. Do not store free-form outcome reasons; the constrained `outcome`
is the complete machine-readable reason.

An ineligible, missing, stale, retired, resolved, already-suppressed, or incomplete
candidate is a completed candidate decision, not a reason to leave the row pending.
When no pending rows remain, assert the persisted processed count equals the total
before marking the job completed.

Increment exactly one parent outcome counter with each terminal candidate update in
the same transaction. Add a defensive completion check that also compares the sum of
all outcome counters to `total_candidates`; an inconsistency is a terminal invariant
error, not a reason to rewrite counters from mutable current state.

#### Step 7: Derive counters and queue logs from committed creation results

Extend `PollCycleResult` with the total number of physically created jobs and a
breakdown for `source_context_changed` and `alert_became_eligible`. Increment these
values only when the creator returns `created=true`.

Build `recheck_job_queued` events from the same creation results retained by the poll
transaction and emit them only after commit. Do not re-query jobs by timestamp to
reconstruct what was created. The poll summary total must equal the sum of its trigger
breakdown and the number of queued-job events emitted for that committed poll.

Observability-counter collection failures must log their own warning and use a safe
default; they must never roll back source reconciliation, alert transitions, or job
creation.

**Explicit prohibitions**

- Do not keep any late-attachment path for compatibility.
- Do not fix only `trigger_type`, logs, or counters while membership remains mutable.
- Do not use the source context alone as unique job identity.
- Do not read fresh labels or source membership as correctness-critical job input.
- Do not hide unprocessed candidates with an eligibility filter in the load query.
- Do not delete immutable candidate rows when a mutable alert row is retained or
  evicted.
- Do not create a full-scan job for an alert-only eligibility transition.
- Do not add migration or legacy-schema code.

**Acceptance criteria**

- Job rules, candidate membership, and candidate decision input are immutable after
  creation.
- Same-context alert/target eligibility creates a new FIFO successor.
- Old jobs cannot suppress a newer global or target lifecycle episode.
- Every completed job has terminal outcomes for all candidates and consistent totals.
- Every job has truthful, constrained trigger provenance.
- Poll counters and queued-job events come from the same committed creation results.
- Restart reproduces exactly the same pending input and outcomes.
- No production post-creation candidate INSERT remains.
- Canonical fingerprint golden vectors remain stable across restart and input order.

These criteria are the acceptance contract for the implementation. Problem 2 provides the
immutable input boundary. Problem 3 provides retry, scheduler, operator recovery, and the
queue-state remainder.

### 3. Define Attempts, Automatic Retry, Terminal Failure, and Operator Recovery

**Status: implemented**

**Dependency**

Complete Problems 1 and 2 first. Retry and recovery must operate on the same immutable
job row and candidate input; do not implement this state machine on top of mutable
membership.

**Required state model**

Use these job states and no implicit states encoded only by nullable fields:

| State | Meaning | Selected by automatic worker |
|---|---|---|
| `queued` | Job scheduled for an immediate attempt; it may be new or may preserve progress from an operator retry | Yes |
| `running` | At least one batch committed and pending candidates remain | Yes |
| `retry_wait` | A selected attempt failed transiently and is waiting for `next_attempt_at` | Only when due |
| `failed` | Non-retryable failure or exhausted automatic retry budget | No; blocks FIFO |
| `completed` | Every candidate has a terminal outcome | No; terminal |
| `abandoned` | Operator explicitly accepted skipping the remaining work | No; terminal |

Strict FIFO remains mandatory. The oldest `queued`, `running`, `retry_wait`, or
`failed` job owns the head of the queue. A not-yet-due `retry_wait` job and a `failed`
job block newer jobs. Only completion, successful progress, an operator retry of the
same job, or explicit operator abandonment can advance that head.

#### Step 1: Add the state-machine and failure-policy tests first

Add pure table-driven tests for every state/event pair before changing SQL. Include
at least:

- initial selection and first committed batch;
- successful resume after restart;
- pre-selection writer acquisition failure;
- normal shutdown cancellation before and after selection;
- retryable selected-attempt failure;
- not-due and due `retry_wait` selection;
- successful batch after retry;
- consecutive retry-budget exhaustion;
- terminal invariant failure;
- operator retry from `failed`;
- operator abandonment from `failed`;
- duplicate operator request;
- concurrent operator CAS requests;
- completed/abandoned transition rejection; and
- strict FIFO with queued, retrying, failed, completed, and abandoned predecessors.

Factor retry scheduling into a pure function of `(consecutiveFailureCount, now)` and
queue eligibility into a pure function of `(status, nextAttemptAt, now)`. Pass
controlled timestamps in tests; never wait for the real `1s` through `10m` schedule.

Use a package-private clock interface for the scheduler. Production uses the real
clock and timer implementation; tests use a deterministic manual clock from
`_test.go`. Do not expose clock selection through config, `StoreOptions`, or a public
API.

Remove the unused `failedPredecessor` boolean from `RecheckJobCanRun`. Production SQL
and the pure state machine must use the same explicit status/due-time rules. Do not
retain a test-only state dimension that production never supplies.

#### Step 2: Extend baseline schema v1 and preserve attempt history

Modify `silence_recheck_jobs` directly. Add current-state fields for:

- `attempt_count`;
- total `retry_count`;
- `consecutive_retry_count`;
- nullable `next_attempt_at`;
- immutable first error message, class, stage, and timestamp;
- mutable last error message, class, stage, and timestamp;
- `recovery_count`;
- nullable `abandoned_at`, `abandoned_by`, and `abandon_reason`; and
- existing created/started/finished timestamps with CHECK constraints consistent with
  every status.

Use these timestamp rules: `retry_wait` has non-null `next_attempt_at` and null
`finished_at`; `failed`, `completed`, and `abandoned` have non-null `finished_at` and
null `next_attempt_at`; `queued` and `running` have both fields null. `abandoned_at`
is non-null only for `abandoned` and equals `finished_at`. Only `queued` may have a null
`started_at`; failure bookkeeping restores the first selected attempt time after the
candidate transaction rolls back.

Add an append-only `silence_recheck_job_attempts` table. Each selected batch attempt
must have a unique opaque `attempt_token`, per-job attempt number, start/end time,
outcome, failure class/stage, and nullable scheduled retry time. Constrain outcomes
to `committed`, `retry_scheduled`, and `failed`.

An attempt becomes durable only when either the batch transaction commits its
`committed` row or failure bookkeeping commits a `retry_scheduled`/`failed` row.
Pre-selection acquisition failures and shutdown cancellation do not insert rows or
consume an attempt number. Enforce and test:

```text
attempt_count = COUNT(silence_recheck_job_attempts WHERE job_id = current job)
```

Add an append-only `silence_recheck_job_operator_actions` table with:

- job ID;
- opaque request ID;
- action constrained to `retry` or `abandon`;
- nullable actor;
- persisted reason;
- requested/applied timestamp;
- applied result and resulting status; and
- `UNIQUE(job_id, request_id)`.

Store a canonical request fingerprint so reuse of one request ID with a different
action or reason can be rejected rather than mistaken for an idempotent replay. The
versioned canonical input is exactly `{action, normalizedReason, normalizedActor}`.
Authenticate and normalize the actor before looking up an existing request. Replaying
the request under a different actor returns HTTP 409. An exact replay returns the original
operator result and creates no second domain action, event, or scheduler wake. Its committed
Store writer invocation still appends exactly one operational `lifecycle_writer_commits`
row; semantic replay equality excludes only that expected reconciliation-ledger delta.

Do not store labels, matchers, comments, headers, or other sensitive domain payloads
in attempt rows or logs. Preserve sanitized operational error text only.

The first error fields are write-once. Later failures update only last-error fields
and append a new attempt row. A successful batch clears `next_attempt_at` and resets
`consecutive_retry_count` to zero, but it does not erase first-error history or reduce
total `retry_count`.

#### Step 3: Use explicit failure boundaries and typed classification

Define stable stages such as `acquire_writer`, `select_job`, `load_context`,
`load_candidates`, `evaluate`, `persist_candidate`, `update_progress`, `commit`, and
`persist_failure`. Do not use the current generic `worker` stage.

Classify outcomes by typed errors and context state, never by message substring:

| Boundary/class | Job mutation |
|---|---|
| Writer acquisition fails before job selection | None; worker telemetry only |
| Worker context is canceled by normal shutdown | Roll back; no retry count and no failed state |
| SQLite BUSY-family error after durable selection | Retryable, although it should be unreachable after successful `BEGIN IMMEDIATE` |
| Candidate-specific malformed/missing/stale input | Terminal candidate outcome; job continues |
| Corrupt pinned job input, CHECK/FK violation, schema mismatch, or invariant failure | Terminal job failure |
| Unknown unclassified storage/runtime error | Terminal job failure; do not guess that it is transient |

Use `errors.As` to inspect modernc error codes and `errors.Is` for context errors.
The post-selection retryable SQLite allowlist is exactly primary/extended BUSY codes
`5`, `261`, `517`, and `773`. Keep all other codes terminal unless this specification
is reviewed and changed first. Do not retry `SQLITE_LOCKED`, `SQLITE_CORRUPT`,
`SQLITE_NOTADB`, `SQLITE_FULL`, constraint errors, or invalid pinned data.

Create a package-private SQL execution/fault abstraction at the recheck transaction
boundary. Production delegates directly to the writer connection. Tests may provide
an implementation from `_test.go` that deterministically returns a typed error at a
named stage. It must not be reachable from production configuration or public APIs.
Use it only for post-selection retryable failure, commit-error/token-present,
commit-error/token-absent, reconciliation-query failure, and failure-bookkeeping
tests. Do not add SQLite triggers or sleeps as fault injection.

#### Step 4: Implement bounded durable automatic retry

Use five automatic retries after a retryable selected-attempt failure. Schedule the
five waits as `1s`, `5s`, `30s`, `2m`, and `10m`. The sixth consecutive retryable
failure transitions the job to `failed`.

This is a consecutive-failure budget. Any committed batch resets
`consecutive_retry_count` to zero, so a large job is not failed merely because it had
several isolated transient failures over its lifetime. `retry_count` remains the
total historical number of automatic retries scheduled for the job.

Persist `retry_wait`, counters, next-attempt time, error fields, and the attempt row
atomically in a separate writer transaction after the failed batch transaction has
rolled back and released writer ownership. Only then emit `retry_scheduled`.

Use the same pre-generated attempt token and attempt number for failure bookkeeping.
The unique attempt row makes a repeated bookkeeping operation idempotent.

If failure bookkeeping cannot commit, return and log a distinct
`failure_persistence_failed` outcome. Never log `retry_scheduled` or `job_failed` when
the corresponding state was not confirmed on disk.

The scheduler must then retain a process-local pending failure-bookkeeping record and
retry only that bookkeeping transaction with bounded delays of `1s`, `5s`, and then
`30s` while the process remains alive. It must not start another candidate batch for
that job until bookkeeping commits or shutdown begins. If the process exits first,
the unchanged durable job is safely selected and classified again after restart.

#### Step 5: Reconcile commit errors with the attempt token

Generate the opaque attempt token before each selected batch. Insert its `committed`
attempt row inside the same transaction as candidate outcomes and parent progress.

Use one internal commit-reconciliation helper for both the batch transaction and the
separate failure-bookkeeping transaction. Pass the attempt token and expected durable
outcome (`committed`, `retry_scheduled`, or `failed`).

If `Commit` returns an error, use a fresh writer transaction/connection state to look
up that attempt token:

- token present: the batch committed; return the committed result and do not retry;
- token absent: the batch did not commit; classify and persist the failure normally;
- reconciliation query fails: report outcome unknown, leave the immutable job data
  untouched, and let the scheduler verify again before another attempt.

For failure bookkeeping, a token present with the expected attempt number/outcome
means bookkeeping committed and must not be repeated. An absent token permits the
same idempotent bookkeeping transaction to run again. A token with different job,
attempt number, or outcome is a terminal invariant failure. A reconciliation query
failure blocks new candidate batches and retries reconciliation only.

Before reconciliation, discard the uncertain writer connection under `writeMu` and
acquire/configure a replacement as defined in Problem 1. Add
`commit_outcome_unknown` to the typed worker-result vocabulary. While that result is
unresolved, the scheduler may retry only token reconciliation with bounded
process-local backoff; it must not execute another candidate batch.

Never retry only the last SQL statement and never reuse a transaction after any
statement or commit error.

#### Step 6: Add one dedicated recheck scheduler owner

Replace direct worker calls from poll and housekeeping with a single background
recheck scheduler goroutine owned by `cmd/karma`. `OpenStore` remains passive and
never starts goroutines. Application setup starts exactly one scheduler after Store
initialization; shutdown cancels and joins it before `Store.Close`.

Store/package tests and the recorded replay may call the real
`RunSilenceRecheckBatchContext` directly without starting the application scheduler.
The package-private clock is supplied only to the `cmd/karma` scheduler constructor;
production setup always uses the real clock.

Use a capacity-one non-blocking wake channel plus one timer state. The timer is either a
process-local infrastructure backoff or the persisted head job's `next_attempt_at`.
Signal the scheduler on:

- startup, so queued/running/retry-wait work resumes after restart;
- committed job creation;
- committed operator retry or abandonment;
- delayed cross-operation reconciliation that confirms a committed selected
  `recheck_batch` attempt; and
- completion of a wait timer.

An ordinary or immediately reconciled recheck batch is already owned by the active
scheduler and must not wake it. Empty/no-work commits never wake it. A wake received while
either timer kind is active is coalesced and cannot run the worker before the timer event.
Immediate and reconciled committed `recheck_failure` use one operation-specific event
finalizer. It emits the retained selection and terminal-failure event when applicable; a
terminal outcome then releases its terminal-failure finalization barrier. Reconciled failure uses the already-active
reconciliation timer and retained retry/failed result without a finalizer wake. A waiting
Retry or Abandon mutation proceeds only after the failure barrier is released. A successful
operator mutation publishes a separate operator-action finalization barrier before writer
unlock; recheck execution waits for that barrier so `operator_action_applied` is attempted
before operator-released work can be selected or its scheduler wake becomes runnable.

The scheduler may process consecutive batches while runnable work exists, but every
batch must release Store writer ownership before the next batch so poll, resolve,
regroup, and housekeeping can proceed. Stop the loop when the queue is empty, the
head is failed, or the head retry is not due.

For pre-selection writer acquisition failures, use process-local scheduler delays of
`1s`, `5s`, and then at most `30s` while contention continues. Do not persist these as
job retries because no job was selected and the database writer could not be
acquired. A successful acquisition resets this scheduler delay.

Maintain separate process-local backoff owners for acquisition, pre-attempt work,
failure bookkeeping, commit reconciliation, and queue-head reads. Reset only the owner
whose success boundary was reached. A pending failure-bookkeeping or reconciliation record
blocks another candidate batch. A durable failed head is a wakeable FIFO state: the
scheduler stays alive for committed Retry or Abandon. Terminal bookkeeping/invariant
failures terminate the scheduler until application restart.

Do not create one scheduler goroutine per poll, job, or retry. Do not run the old
poll-triggered and housekeeping-triggered workers in parallel with the new owner.

Add deterministic scheduler tests for repeated start/stop, exactly one owner,
startup wake, wake coalescing, replacement of an older timer when the head changes,
due-time wake, process-local acquisition backoff reset, pending failure-bookkeeping
retry, commit reconciliation, cancellation while idle, cancellation during a batch,
and scheduler stop completion before Store close.

Include a test proving that committed abandonment immediately wakes the scheduler and
allows the next FIFO job to start without waiting for a poll, housekeeping run, or
unrelated timer.

#### Step 7: Add operator retry and abandon operations

Add these authenticated application routes following the existing lifecycle route
and actor conventions:

- `POST /alerts-history/recheck/{jobID}/retry`;
- `POST /alerts-history/recheck/{jobID}/abandon`.

Use the authenticated request actor when authentication is enabled; keep it nullable
under the existing no-auth deployment behavior. Limit and validate request bodies.
An abandon request requires a non-empty operator reason; a retry request may include
an optional reason. Persist reasons for audit but never include them in runtime logs.

Both operations must be idempotent semantic CAS operations on the same job row:

- retry applies only from `failed`, transitions to `queued`, clears the active
  schedule, increments `recovery_count`, and preserves rules, candidates, progress,
  first error, attempts, and FIFO position;
- abandon applies only from `failed`, transitions to `abandoned`, records actor/time
  and reason, sets `next_attempt_at=NULL` and `finished_at=abandoned_at`, preserves all
  job input/history, and releases the FIFO head;
- replay of the same request ID and canonical payload returns the originally recorded
  HTTP 200 body, including its original `applied` value;
- an invalid state or different concurrent action returns HTTP 409;
- an unknown job returns HTTP 404.

Both request bodies must be strict JSON and reject unknown fields. Limit each body to
4096 bytes. Use these exact shapes:

```json
{"requestId":"opaque-client-id","reason":"optional retry reason"}
```

```json
{"requestId":"opaque-client-id","reason":"required abandon reason"}
```

`requestId` is required, trimmed, 1-128 ASCII characters, and limited to letters,
digits, `.`, `_`, `:`, and `-`. Reasons are trimmed UTF-8 strings of at most 1024
bytes. Whitespace-only abandon reason is invalid. Return HTTP 400 for malformed job
ID, invalid JSON, unknown fields, invalid request ID, oversized body/reason, or a
missing abandon reason.

Successful and idempotently replayed requests return this exact response shape:

```json
{
  "status": "success",
  "jobId": 547,
  "action": "retry",
  "requestId": "opaque-client-id",
  "applied": true,
  "currentStatus": "queued"
}
```

Use the same shape with `action="abandon"` and
`currentStatus="abandoned"`. Error responses use the existing JSON error envelope
and the HTTP status rules below; never return a partially populated success object.
Every successful first CAS and exact replay returns `applied:true`; there is no HTTP
200 `applied:false` branch in this API.

Persist the operator-action row in the same transaction as the semantic status CAS.
Repeating the same `(jobID, requestId)` and identical canonical payload returns the
recorded HTTP 200 result after restart. Reusing that ID with a different action or
reason returns HTTP 409. A new request ID against a state that no longer permits the
action also returns HTTP 409.

Operator retry must set `status='queued'`, `next_attempt_at=NULL`,
`consecutive_retry_count=0`, and `finished_at=NULL`. Preserve `started_at`, all
candidate progress, `attempt_count`, total `retry_count`, first/last error history,
and existing attempt rows.

When authentication is enabled, a mutation request without an authenticated actor
returns HTTP 401. When authentication is disabled, the existing deployment contract
permits the action with a NULL actor. The UI follows the same rule: show controls when
authentication is disabled or when an authenticated user is present.

Do not create a replacement job for operator retry. A replacement would receive a
new FIFO position and lose the causal relationship to existing progress.

Add Store CAS tests, HTTP handler tests, concurrent request tests, restart tests, and
redaction tests. Wake the scheduler only after a retry transaction commits.

The API suite must cover malformed and unknown IDs, oversized and unknown JSON
fields, whitespace-only reasons, 401/404/409 responses, retry-versus-abandon races,
double submission, and replay of the same request ID after Store restart. Run the
relevant scheduler and operator concurrency tests under `go test -race`.

**Explicit prohibitions**

- Do not count pre-selection writer acquisition as a job attempt or retry.
- Do not mark normal shutdown cancellation as failed.
- Do not automatically move a `failed` job back to runnable state.
- Do not let a newer job bypass a failed or not-yet-due FIFO head.
- Do not overwrite the first root error.
- Do not invent a generic version column; use semantic status/counter predicates for
  CAS.
- Do not delete or recreate job rules/candidates during retry or recovery.
- Do not retry a statement inside a failed transaction.
- Do not leave retry timing only in memory.
- Do not add migration or compatibility code.

**Acceptance criteria**

- Pre-selection infrastructure failure cannot poison a job.
- Retry state, schedule, counters, and errors survive restart.
- Automatic retries are bounded and use the documented schedule.
- Successful progress resets only the consecutive failure budget.
- Terminal failure blocks strict FIFO until an explicit operator decision.
- Operator retry resumes the same job and preserves all immutable input and progress.
- Operator abandonment is explicit, audited, idempotent, and releases FIFO.
- Commit uncertainty cannot duplicate candidate outcomes, promotions, events, or
  suppression evidence.
- Every logged retry/failure/recovery state was confirmed by a committed transaction.
- Failure-bookkeeping and commit-reconciliation errors cannot start a hot loop or a
  second candidate attempt.

The current Problem 3 implementation adds the six-state durable job machine,
append-only attempt ledger, bounded retry schedule, token reconciliation, one cmd-owned
scheduler, and authenticated semantic-CAS retry/abandon routes. The capability-based test
matrix is recorded in `architecture-verification.md`.
Acceptance must not be inferred from the presence of the schema tables or local green
suites.

After these criteria pass, update the phase-2 schema/plan, lifecycle state machine,
writer projection audit, architecture conformance/verification, API route contract,
and implementation plan with the implemented state/scheduler/operator contract.

### 4. Correct Recheck Observability and Operator Presentation

**Status: Implemented and independently accepted.**

**Dependency**

Complete Problems 1-3 first. This problem presents the already-approved job and
attempt state. It must not define a second retry policy, create new statuses, change
candidate membership, or repair provenance independently.

**Additional confirmed observability defects**

- Regroup failure persistence currently emits an infrastructure event before an
  unknown Commit outcome is reconciled. The Store must own only durable
  `regroup_failed`; cmd owns the single terminal infrastructure event.
- Between-batch regroup cancellation can escape as raw `ctx.Err()`, losing typed
  expected-shutdown cause and producing the wrong event/level.
- Global and target `candidate_promoted` events omit the transition ID already returned
  by the promotion helpers.
- Cmd `worker_failed` can duplicate Store-owned selected-job events, and
  `batch_promoted` duplicates `batch_completed`.
- Poll observability lacks injectable best-effort metadata boundaries and complete
  deterministic tests for transaction-local event ownership, all event families,
  delayed finalizers, required fields, and redaction.

These defects are real but must not expand the implemented Problem 1 contract. Resolve
them only after the Problems 2-3 domain/state contracts are implemented.

#### Step 1: Return a typed worker result

Replace the current `(promoted int, error)` contract with a result that distinguishes:

- `queue_empty`;
- `batch_committed`;
- `retry_scheduled`;
- `retry_deferred`;
- `queue_blocked`;
- `acquisition_failed`;
- `infrastructure_failed`;
- `commit_outcome_unknown`;
- `job_failed`; and
- `failure_persistence_failed`.

Include only metadata already established by committed state: job ID, status, queue
depth, processed/promoted/skipped counts, attempt/retry counts, next-attempt time,
trigger type, and stable failure class/stage. A persisted failed predecessor is a
`queue_blocked` result, not an execution error.

#### Step 2: Assign one owner to every runtime event

Use this event ownership and level contract:

| Situation | Event behavior |
|---|---|
| Pre-selection acquisition failure | One `worker_acquisition_failed`; warning on edge, debug while unchanged |
| Job selected for a real attempt | `job_selected` debug after runnable/due validation |
| Committed successful batch | Existing started/resumed, batch, candidate, and completion events after commit |
| Durable retry scheduled | One `retry_scheduled` warning with job ID, stage, counts, and next attempt |
| Retry head not due | `retry_deferred` debug; do not emit an error per scheduler wake |
| Durable terminal failure | One `job_failed` error after failure-state commit |
| Persisted failed head | `failed_job_blocks_queue` warning on edge, debug while unchanged |
| Operator retry/abandon | One committed info event with job ID and operator; never log reason text |

Do not emit `job_selected`, `job_failed`, or outer `worker_failed` for a persisted
blocker. Remove duplicate wrapper error logging when the Store/scheduler result already
owns the state transition event.

`queue_blocked` and `retry_deferred` are successful typed results with no invocation
error. A selected terminal infrastructure outcome uses the non-durable
`infrastructure_failed` disposition and is emitted exactly once by cmd as
`job_infrastructure_blocked`; pre-selection acquisition uses only
`worker_acquisition_failed`. The registered batch reason codes are
`writer_unavailable`, `writer_invariant`, `rollback_failed`, `store_closing`,
`store_closed`, `worker_cancelled`, `worker_deadline`, `infrastructure_unclassified`,
and `queue_read_terminal`. Failure-persistence reason codes use the corresponding
`failure_persistence_*` namespace. Commit uncertainty and confirmed non-commit remain
Problem 3 reconciliation/failure paths and are not terminal infrastructure reasons.

Edge-sensitive logging state must reset when the head job or head status changes. It
must not be the source of domain truth and does not need to survive restart; the
persisted job/attempt state is authoritative.

#### Step 3: Replace flat recheck stats with an explicit shape

Expose queue depth and an optional head-job object from `/alerts-history/stats`. The
head object must include:

- job ID, trigger type, status, created/started/finished timestamps;
- total and processed candidate counts;
- promoted and each skipped/outcome count;
- attempt count, total retry count, consecutive retry count;
- nullable next-attempt time;
- first error class/stage/time/message;
- last error class/stage/time/message; and
- recovery count.

Use JSON fields `recheck.queueDepth` and nullable `recheck.headJob`. When no
nonterminal queue head exists, `headJob` is JSON `null`, including after the last job
is completed or abandoned. Do not return a completed/abandoned row merely because it
is the latest historical job. Add a separate historical summary later only through a
new approved contract.

Use this exact nested shape; `firstError` and `lastError` are either the shown object
or JSON `null`, and all timestamps are RFC3339Nano UTC strings:

```json
{
  "recheck": {
    "queueDepth": 1,
    "headJob": {
      "jobId": 547,
      "triggerType": "alert_became_eligible",
      "status": "retry_wait",
      "createdAt": "2026-08-16T12:00:00Z",
      "startedAt": "2026-08-16T12:00:01Z",
      "finishedAt": null,
      "totalCandidates": 20,
      "processedCandidates": 5,
      "outcomeCounts": {
        "promoted": 3,
        "stale": 1,
        "ineligible": 1,
        "missingLabels": 0,
        "incompleteSources": 0
      },
      "attemptCount": 2,
      "retryCount": 1,
      "consecutiveRetryCount": 1,
      "nextAttemptAt": "2026-08-16T12:00:06Z",
      "firstError": {
        "class": "transient_infrastructure",
        "stage": "persist_candidate",
        "at": "2026-08-16T12:00:01Z",
        "message": "sanitized error"
      },
      "lastError": {
        "class": "transient_infrastructure",
        "stage": "persist_candidate",
        "at": "2026-08-16T12:00:01Z",
        "message": "sanitized error"
      },
      "recoveryCount": 0
    }
  }
}
```

The enclosing storage-stats response keeps its other existing non-recheck fields.
The `recheck` object is always present, including disabled storage, where it is
`{"queueDepth":0,"headJob":null}`. Remove the old flat `recheckCurrent*` and
`recheckOldestCreatedAt` fields in the same backend/frontend change; do not emit both
shapes.

Do not expose raw rule payloads, matcher values, labels, comments, attempt tokens, or
abandon reason. Keep full operator audit data in storage and safe server logs.

Stats collection is observational. A count or `RowsAffected` failure while collecting
stats must not change queue processing, retry scheduling, or any transaction outcome.

#### Step 4: Replace the scalar warning response with typed issues

The current single `reason` field cannot represent regroup error, HA inconsistency,
and recheck failure at the same time. Replace it with an `issues` array. Each issue
must have a stable `kind`, severity, human-readable summary, and kind-specific safe
metadata.

Support at least:

- `regroup_out_of_date`;
- `regroup_in_progress`;
- `regroup_failed`;
- `silence_ha_inconsistent`;
- `recheck_retrying`; and
- `recheck_blocked`.

Use this top-level response shape and remove the old scalar `reason`, regroup,
silence-cluster, and recheck fields rather than returning two sources of truth:

```json
{
  "status": "ok",
  "storageEnabled": true,
  "dbExpectedState": false,
  "issues": [
    {
      "kind": "recheck_blocked",
      "severity": "error",
      "summary": "Silence recheck queue is blocked",
      "metadata": {
        "jobId": 547,
        "status": "failed",
        "firstFailureStage": "persist_candidate",
        "firstFailureAt": "2026-08-16T12:00:00Z"
      }
    }
  ]
}
```

The top-level JSON fields are exactly `status` (string), `storageEnabled` (boolean),
`dbExpectedState` (boolean), and `issues` (array). Return `issues: []`, never `null`,
when expected state is healthy or storage is disabled. Every timestamp below is an
RFC3339Nano UTC string. Integer counts and IDs are JSON numbers. Metadata objects
contain exactly the fields listed here:

`severity` is exactly `warning` or `error`. Sort issues by error before warning and
then by `kind` lexically so API and UI tests are deterministic. Metadata is a strict
discriminated union:

| Kind | Severity | Required metadata |
|---|---|---|
| `regroup_out_of_date` | warning | `storedFingerprint:string`, `expectedFingerprint:string` |
| `regroup_in_progress` | warning | `processed:number`, `total:number` |
| `regroup_failed` | error | `error:string`, `processed:number`, `total:number` |
| `silence_ha_inconsistent` | warning | `targetNames:string[]` sorted lexically from logical Alertmanager target/cluster IDs |
| `recheck_retrying` | warning | `jobId:number`, `status:"retry_wait"`, `retryCount:number`, `nextAttemptAt:string`, `lastFailureStage:string` |
| `recheck_blocked` | error | `jobId:number`, `status:"failed"`, `firstFailureStage:string`, `firstFailureAt:string` |

Do not add kind-specific fields at the issue top level and do not include fields from
another metadata variant. Do not expose operator reasons or raw candidate/rule data
in warning metadata.

`dbExpectedState` is true only when the issues array is empty. Do not preserve the old
scalar response as a compatibility fallback; there are no deployed consumers that
require it.

Update `ForkLifecycleWarning` so it does not describe every issue as a grouping
problem. Render every simultaneous issue with the correct semantics. For recheck:

- running work is informational and does not produce a warning by itself;
- retry waiting is a persistent warning with job ID, retry count, and next attempt;
- failed head is a persistent error with job ID and first failure stage;
- authenticated operator controls offer Retry and Abandon only for a failed head;
- Abandon requires explicit confirmation and a reason;
- controls disable while a request is in flight and refresh from the server result.

When authentication is enabled, hide controls unless the current UI session has an
authenticated username. When authentication is disabled, show the controls and let
the server record a NULL actor. On HTTP 409, close no warning state locally; refresh
the server warning response and render the authoritative result. Prevent double click
while a request is in flight and require explicit Cancel/Confirm handling for
abandonment.

Follow the existing visual language. If visible colors or states change, run the
required light/dark rendered-state review for resting, hover, focus, disabled,
loading, warning, and error states.

Add UI package script `test:visual` with command
`playwright test src/e2e/visual.spec.ts` so the visual suite always runs with `ui` as
its package root.

#### Step 5: Add sequence tests at every observability layer

Add Store log-capture tests and `cmd/karma` wrapper tests for exact event sequences:

- first failure transition;
- repeated persisted blocker reads;
- transient retry scheduling and repeated not-due wakes;
- successful retry;
- operator retry and abandonment; and
- change from one blocked head to another.

Assert component, event, level, job ID, status, stage, retry count, and edge behavior.
Assert that labels, matchers, comments, operator reasons, raw SQL, and attempt tokens
never appear.

Add stats and warning HTTP response tests for empty, running, retrying, failed,
completed, abandoned, multiple simultaneous warning issues, and disabled-storage
states. Add API negative tests for malformed values and strict union serialization.
Add UI tests for every rendered issue, authentication-enabled/disabled controls,
cancelled confirmation, double click, loading, request failure, HTTP 409 refresh, and
both successful operator actions.

Use the package-private observability/fault abstraction to make stats or logging
metadata collection fail immediately before and after a committed domain transaction.
Assert that domain rows, scheduler decisions, and API operation results are unchanged.

**Acceptance criteria**

- Logs distinguish an invocation failure, a real job attempt, retry waiting, and a
  persisted queue blocker.
- No historical error is emitted as a new active failure on every poll or wake.
- Stats expose the complete safe head-job state defined by Problem 3.
- Warning API represents simultaneous independent issues without overwriting one.
- UI text and controls match the actual issue rather than always claiming regroup.
- Operator actions are visible only in valid states and remain server-authoritative.
- Observability failures never change domain outcomes.
- Warning JSON has one authoritative strict shape and deterministic issue ordering.

After these criteria pass, update the runtime logging, stats API, warning API, UI,
architecture conformance/verification, and implementation-plan records for the
implemented presentation contract.

### 5. End-to-End Verification and Documentation Gate

**Status: Implemented and independently accepted.**

**Purpose**

This is a final integration and traceability gate. It introduces no new job states,
retry policy, creator behavior, counters, logs, API semantics, or UI decisions.
Focused regressions belong to Problems 1-4, 8, and 9 and must already pass before this
gate.

#### Step 1: Verify phase-boundary records and ADRs

Confirm that the Step 0 known-violation markers were replaced only when their owning
problem passed its production tests. Verify that each ADR records the final decision,
rejected alternatives, transaction owner, restart behavior, and verification links.
Do not repair missing phase documentation silently in this final gate; return to the
owning problem, update it there, and rerun that problem's checks first.

#### Step 2: Add one complete small deterministic lifecycle sequence

The integration sequence must cover:

1. source-context job creation with truthful provenance and counters;
2. partial batch commit;
3. same-context alert/target eligibility successor creation;
4. Store close/reopen with immutable pending input;
5. transient selected-attempt failure and durable retry wait;
6. retry after restart and successful continuation;
7. terminal failure at the FIFO head;
8. repeated scheduler wakes with edge-sensitive blocker logging;
9. operator retry and operator abandonment paths;
10. stats and warning HTTP responses; and
11. exact final job/candidate/attempt/event/evidence rows.

Use deterministic barriers, controlled timestamps, and explicit transaction
boundaries. Do not use sleeps to decide ordering. Compare persisted rows before and
after restart.

#### Step 3: Extend the recorded-traffic replay through the real worker

Extend `cmd/karma/fork_alertlifecycle_replay_test.go`; do not replace recorded traffic
with a small synthetic-only fixture.

The replay must:

1. load the recorded alert output and persist the initial live poll;
2. sort candidate scopes by `(alert_id, target_key)` and select at least 80 recorded
   scopes with original match labels;
3. use worker batch size 20 so the source-triggered job requires at least three real
   batches;
4. apply a deterministic active silence source context and create a
   `source_context_changed` job after complete disappearance;
5. before processing, partition explicit recorded scopes into expected outcomes:
   unchanged matching scopes become `promoted`, re-fired scopes become stale
   observation, manually resolved scopes become `already_resolved`, and nonmatching
   scopes become `no_matching_rule`;
6. run one real batch, close/reopen the Store, and finish the remaining batches;
7. under an unchanged source context, make a disjoint recorded subset eligible and
   assert a separate `alert_became_eligible` successor; both trigger types are
   mandatory and `as applicable` is not permitted;
8. create one later promotion-capable source context where a source relevant to a
   fixed recorded subset is pinned failed, then assert
   `incomplete_source_coverage` for that subset;
9. verify exact outcome rows, aggregate counters, immutable candidate counts, pinned
   observation tokens, and fingerprints before and after restart;
10. verify suppression events and exact cause revisions; and
11. verify merge/preview output for promoted and untouched recorded alerts.

The replay must process enough recorded alerts to exercise multiple batches. It must
not call private promotion helpers directly or replace the production worker with an
in-memory evaluator.

#### Step 4: Update all authoritative documents to the implemented contract

After production code and all regressions pass, reconcile at least:

- `fork-docs/ARCHITECTURE.md`;
- `fork-docs/IMPLEMENTATION_PLAN.md`;
- `fork-docs/SPEC/architecture-conformance.md`;
- `fork-docs/SPEC/architecture-verification.md`;
- `fork-docs/SPEC/implementation-contracts.md`;
- `fork-docs/SPEC/phase2-silence-awareness-plan.md`;
- `fork-docs/SPEC/phase2-silence-awareness-schema.md`;
- `fork-docs/SPEC/storage-schema.md`;
- `fork-docs/SPEC/lifecycle-state-machine.md`;
- `fork-docs/SPEC/writer-projection-audit.md`;
- `fork-docs/SPEC/runtime-logging-contract.md`;
- `fork-docs/SPEC/api-contracts.md`;
- `fork-docs/SPEC/ui-contracts.md`; and
- `fork-docs/SPEC/state-model.md`.

Every conformance row must name its runtime owner and focused test. Remove obsolete
descriptions of late candidate attachment, source-context-only uniqueness, the
`failedPredecessor` boolean, poll/housekeeping direct worker calls, flat warning
reasons, and repeated wrapper errors.

#### Step 5: Run final verification in the required order

Run focused tests while developing, then execute all final gates from the repository
root after the last edit:

```bash
go test ./internal/fork/alertlifecycle -count=1
go test ./cmd/karma -count=1
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 180s
go test -race ./cmd/karma -count=1 -timeout 180s
npm --prefix ui test -- --runInBand \
  src/Components/ForkLifecycleWarning/index.test.tsx \
  src/Components/MainModal/Configuration/ForkStorageStats.test.tsx
npm --prefix ui run build
npm --prefix ui run test:visual -- --grep 'Fork lifecycle warning'
make lint
go test ./... -count=1 -timeout 180s
git diff --check
```

The Problem 4 visual project is intentionally scoped to the installed Chromium,
Firefox, and Android Playwright projects; WebKit and iPhone exclude this feature in
`ui/playwright.config.ts` because their host dependencies are unavailable. The scoped
suite must independently render every changed warning state in both themes and assert
computed issue-card/button colors, real border style/width, text and non-text contrast,
and horizontal-overflow safety. Do not record measured results until those exact rendered
states and visible boundaries pass.

Add a local light/dark visual fixture named `Fork lifecycle warning` to
`ui/src/e2e/visual.spec.ts`. It must render every resting, retrying, blocked, loading,
disabled, hover, focus, confirmation, success, and error state changed by Problem 4.
Record actual foreground/background/border colors and contrast results in the final
verification record.

Run this exact Cyrillic scan from the repository root; any match is a failure:

```bash
if grep -RInP '[\x{0400}-\x{04FF}]' \
  --include='*.go' --include='*.ts' --include='*.tsx' \
  --include='*.js' --include='*.jsx' --include='*.md' \
  --include='*.yml' --include='*.yaml' --include='*.json' \
  --include='*.jsonc' --include='*.scss' --include='*.css' \
  --include='*.html' --exclude-dir=.git --exclude-dir=node_modules \
  --exclude-dir=dist .; then
  exit 1
fi
```

Do not report completion if a focused test passes but the recorded-traffic replay,
race suite, visual review, full Go suite, root lint, UI build, Cyrillic scan, or
patch-integrity check fails.

### 6. Harden PID Lease Recovery and Managed-Harness Finalization

**Status: Implemented and independently accepted.**

**Dependency and scope**

This is a separate infrastructure change discovered while reviewing Problem 1. It does
not cause the SQLite lock-upgrade failure and does not change lifecycle domain behavior.

**Resolved defects**

- `pidFileLease.Release` retries identity capture while retaining ownership after a
  transient failure; no path is removed until identity is proven.
- The Windows release adapter joins temporary current-path and owned-handle Close
  errors on both success and failure branches.
- The managed wrapper performs cleanup before publishing its exit marker and records
  cleanup or marker failures as harness failures before a waiter can report success.

**Implemented contract**

- Retry identity capture from a retained valid handle; preserve ownership on transient
  failure and remove nothing until identity is proven.
- Join every temporary/current/owned handle Close error without deleting a replacement
  path.
- Route both platform release adapters through one platform-neutral release state
  machine whose injected primitives cover identity capture, current-path inspection,
  deletion, and current/owned handle close outcomes.
- Classify ownership loss at the exact boundary that proves it. A retained-handle
  identity-read error remains retryable unless the adapter explicitly marks definitive
  loss; current-path absence and confirmed reparse replacement may clear ownership.
- Publish managed final exit only after termination cleanup is complete. Atomically
  persist cleanup/marker failure as harness failure before a waiter may report success.
- Add deterministic primitive-seam tests for retry, ownership loss, Close-error joins,
  and cleanup/exit ordering. Native Windows execution is not required; shared tests and
  Windows cross-compilation are sufficient in the available environment.

Implemented in `cmd/karma/pid_lease_*` and the managed test harness. The accepted
verification matrix and completion commands are recorded in
`fork-docs/SPEC/architecture-verification.md`.

### 7. Reconcile the Completed Recheck-Job Retention Documentation

**Status: Implemented and independently accepted.**

**Dependency and scope**

This documentation-contract problem was discovered while independently reviewing
Problem 2. It is not part of immutable job creation, candidate processing, or Problem 2
acceptance and is kept as a separate documentation change.

**Implemented contract**

Decision D21 in `phase2-silence-awareness-plan.md` defines reference-based completed-job
retention. The explicit schema-v1 status policy is that `queued`, `running`, `retry_wait`,
`failed`, and `abandoned` jobs are retained regardless of age; completed jobs referenced
by global or target suppression-cause history are retained; the latest completed job is
retained as bounded post-run diagnostic context; and older completed unreferenced jobs
and their job-owned graph are removed by the next housekeeping without a separate job
TTL. The stats and warning readers do not expose the latest completed job as the current
queue head.

The authoritative summaries now use this same finite status set and distinguish alert
retention from silence-history retention. Silence-history timestamps are `retired_at`
for retired sources, `last_observed_at` for revisions and rules, and `ended_at` for
closed episodes.

**Contract and evidence**

- Preserve D21 as the decided runtime contract unless a separate architecture decision
  explicitly supersedes it; do not silently add a job TTL during documentation cleanup.
- State consistently that `queued`, `running`, `retry_wait`, `failed`, and `abandoned`
  jobs are retained regardless of age; the accepted `abandoned` retention policy is
  explicit and is not an accidental consequence of a generic predicate.
- State consistently that jobs referenced by global or target suppression-cause history
  and the latest completed job as bounded post-run diagnostic context are retained;
  stats and warning readers do not read the latest completed job as queue head.
- State consistently that other completed unreferenced jobs are deleted by the next
  housekeeping. Job-owned rules, candidates, attempts, and operator-action rows are
  part of that deletable graph and may cascade.
- Limit `alertLifecycle.silenceHistory.retention.ttl` to unreferenced retired sources,
  revisions, closed episodes, and rules. It is independent from alert retention but is
  not a completed-job retention promise.
- `CONFIGURATION.md`, `phase2-silence-awareness-plan.md`,
  `phase2-silence-awareness-schema.md`,
  `lifecycle-state-machine.md`, `writer-projection-audit.md`,
  `retention-and-vacuum.md`, `storage-schema.md`, and ADR-003 must state one contract.
- Keep the current immediate-cleanup regressions. Existing tests are cited by the
  capability they actually execute; SQL/FK inspection establishes cause-reference
  protection and attempts/operator-action cascade rather than being attributed to a
  partial test.

Problem 7 does not block Problem 2 acceptance and remains a separate documentation-only
remediation after the ordered lifecycle work. It introduces no runtime, schema,
configuration, or retention-policy behavior. Problem 10 remains a separate housekeeping
recheck-executor documentation issue.

### 8. Preserve Selected-Error Precedence During Shutdown

**Status: Implemented and independently accepted.**

This problem is independent from Problem 4 presentation. It covers ordinary retryable or
terminal job failures that belong to durable failure bookkeeping. Typed writer
lifecycle, rollback, and invariant infrastructure remain non-durable Problem 4 result
classification and are explicitly excluded here.

The pre-remediation recheck worker checked `ctx.Err()` after an ordinary
selected-operation error and could relabel that earlier error as normal shutdown
cancellation if shutdown won the small classification race. The batch transaction had
already rolled back, but the real retryable or terminal failure was not persisted and
the attempt was repeated after restart without its required durable history.

The controlling Problem 3 rule needs an explicit causal precedence:

- if the worker operation itself returns cancellation/deadline before another failure,
  roll back with no retry count and no failed state;
- if an ordinary retryable/terminal selected-operation failure is established first,
  later shutdown must not erase it. Persist its ordinary attempt result through the
  existing bounded failure-bookkeeping ownership before releasing the scheduler;
- a genuinely unknown Commit outcome takes precedence over both and remains owned by
  durable reconciliation; and
- context state alone must never replace a typed operation cause.

Implemented work:

1. `TestProblem8SelectedErrorPrecedenceDuringShutdown` covers cancellation-first and
   selected retryable/terminal error-first with durable status and attempt assertions.
2. `TestProblem8UnknownCommitPrecedesLaterCancellation` covers unknown Commit precedence.
3. The worker preserves the typed selected cause and uses a bounded internal bookkeeping
   context only after later cancellation; normal cancellation remains non-durable.

Problem 8 does not block independent acceptance of the otherwise-correct Problem 4
presentation, but it must close before Problem 5 signs off end-to-end recheck behavior.

### 9. Complete Generic Writer Diagnostic Redaction and Event Registration

**Status: Implemented and independently accepted.**

This problem is independent from Problem 4 recheck/regroup presentation. The generic writer
now uses a dedicated registered `lifecycle_writer/writer_observability_failed` event for
rollback recovery, panic cleanup, post-commit handoff, and finalizer failures. The old raw
diagnostic and contradictory `observability_collection_failed` ownership are removed.

Implemented work:

1. `TestProblem9GenericWriterObservabilityFailureIsSafeAndRegistered` covers handoff,
   finalizer panic, and rollback recovery with raw-value redaction assertions.
2. Existing writer panic/rollback regressions prove logging cannot change transaction,
   reconciliation, handoff, or Store lifecycle outcomes.
3. Writer ownership, Commit reconciliation, retry policy, Problem 4 API/UI, and schema
   remain unchanged.

Problem 9 does not change Problem 4 semantics; its local remediation is implemented and
independently accepted under the final runtime-logging and end-to-end gate.

### 10. Remove the False Housekeeping-Owned Recheck Batch Documentation

**Status: Implemented and independently accepted.**

This problem is independent from Problem 7 retention. The corrected
`CONFIGURATION.md` describes housekeeping as retention/vacuum work only; runtime
housekeeping invokes `RunHousekeepingContext`. The accepted Problem 3 scheduler is the
sole recheck executor; housekeeping neither executes a batch nor owns a scheduler wake.

Implemented correction:

- the recheck-batch claim is removed from the housekeeping interval description;
- keep immediate startup and periodic housekeeping cadence unchanged;
- point recheck execution semantics to ADR-007 and the scheduler owner;
- do not change runtime scheduling, timers, signals, configuration keys, or defaults;
  and
- no other configuration summary assigns recheck execution to housekeeping.

## Execution Order and Scope Guard

Execute the work in this exact order:

1. Problem 1 writer boundary and fail-fast PID verification
   (completed);
2. Problem 2 immutable job creation and provenance;
3. Problem 3 attempts, retry, failure, and operator recovery;
4. Problem 4 observability and operator presentation (implemented and independently accepted);
5. Problem 8 selected-error/shutdown precedence (implemented and independently accepted);
6. Problem 9 generic writer diagnostic redaction/registration (implemented and independently accepted);
7. Problem 5 end-to-end verification and documentation (implemented and independently accepted);
8. Problem 6 PID lease/harness hardening (completed); and
9. Problem 7 completed-job retention documentation reconciliation (completed); and
10. Problem 10 housekeeping/recheck ownership documentation correction (completed).

Finish the production code, focused regressions, and required documentation for one
problem before implementing the next. Problems 2 and 3 must share one approved
final baseline schema design before either schema edit is made. Problem 2 implements
its portion after its failing tests; Problem 3 implements the approved state/attempt
portion after its own failing tests. Both edit schema v1 directly without migration
or compatibility code.

Do not make compatibility migrations because no deployed lifecycle database exists.
Do not combine unrelated cleanup or upstream refactoring with this work. Commit only
at completed problem boundaries after that problem's required tests pass.

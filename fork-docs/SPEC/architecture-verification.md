# Architecture Verification

This file records reproducible gates for the lifecycle/silence-editor architecture.
It is evidence, not a replacement for the contracts in the state-machine specs.

## Domain and storage

```sh
go test ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test ./internal/config -count=1 -timeout 240s
go test ./cmd/karma -count=1 -timeout 240s
```

Coverage includes pure/fuzz lifecycle invariants, target topology, sticky multi-job
causes, FIFO failure blocking, HA inconsistency, fresh schema v1 bootstrap/reopen,
restart, schema-layout fingerprint validation, and the recorded traffic replay
(`TestRecordedTrafficReplayThroughLifecycleStore`). Problems 1-10 are implemented and
independently accepted.

Historical capability labels retained in ADR prose and executable test names map as
follows: Problem 1 is SQLite writer ownership; Problem 2 is immutable recheck input;
Problem 3 is attempts, retry, and operator recovery; Problem 4 is observability and
operator presentation; Problem 5 is end-to-end replay evidence; Problem 6 is PID lease
and managed-harness finalization; Problem 7 is completed-job retention documentation;
Problem 8 is selected-error versus shutdown precedence; Problem 9 is generic writer
logging isolation; and Problem 10 is housekeeping ownership documentation.

SQLite writer ownership is covered by:

- `TestDeferredTransactionReturnsBusyCodeWhenWriterIsHeld` and
  `TestDeferredTransactionReturnsSnapshotCodeAfterWriterCommit`, which distinguish
  SQLite primary code `5` from extended snapshot code `517`;
- `TestWALReaderContinuesWhileImmediateWriterIsOpen`, which preserves read
  concurrency;
- `TestConfiguredStoreWriterTransactionsStartImmediately`,
  `TestRecheckWaitsForWriterBeforeSelectingJob`, and
  `TestRecheckCannotSelectJobBeforeWriterTransactionStarts`, which verify immediate
  acquisition before queue selection;
- `TestRecheckWriterAcquisitionFailureLeavesQueuedJobUntouched`, which proves a
  pre-selection failure cannot poison a job; and
- `TestWriterConnectionReplacementReestablishesConnectionLocalSettings`,
  `TestWriterTransactionRollsBackOnCallbackError`,
  `TestWriterTransactionRollsBackOnPanic`,
  `TestWriterRollbackIsOwnedByHelperExactlyOnce`,
  `TestWriterCommitErrorWithDurableTokenReconcilesAsSuccess`,
  `TestWriterCommitErrorWithoutDurableTokenIsNotUnknown`,
  `TestWriterCommitLookupFailureBlocksMutationUntilLaterReconciliation`, and
  `TestCloseWaitsForWriterAndRejectsLaterMutation`, which cover replacement,
  typed commit outcomes, reconciliation blocking, rollback ownership, and lifecycle
  cleanup;
- `TestHousekeepingCleanupAndVacuumUseSeparateWriterTransactions`,
  `TestRegroupBeginBatchAndCompletionUseRealWriterPhases`,
  `TestRegroupResumePreservesCursorAndProgress`,
  `TestRegroupFailureWritesErrorMetadataThroughWriter`,
  and `TestResolveWriterPhaseUpdatesGlobalAndAllTargetRows`, which exercise real
  mutation phases with separate transaction-start barriers;
- `TestOpenStorePartialFailuresCloseAcquiredResourcesInOrder`, which verifies
  partial startup cleanup after each acquired resource;
- `TestWriterCommitLedgerUsesExplicitHousekeepingRetention` and
  `TestOpenStoreRejectsUnknownApplicationView`, which cover ledger retention and
  view-aware schema fingerprint rejection;
- `TestRecheckUnknownCommitDoesNotPersistFalseFailedJob`, which proves an unknown
  commit cannot be converted into a persisted failed job or a duplicate mutation.
- `TestRegroupWriterAcquisitionFailureDoesNotPersistDomainFailure`, the recheck
  acquisition aggregate regression, and the bad-connection/cancellation acquisition
  tests, which preserve domain state and classify writer acquisition as infrastructure;
- `TestWriterCancellationBeforeLedgerInsertWithoutHookDoesNotCommit`, together with
  the hook-boundary cancellation regression, which proves the production no-hook path
  uses the same rollback contract and context-bound ledger insert;
- `TestManagedKarmaStartupTimeoutReapsLiveChildWithoutPID`, which manually releases
  the startup deadline after a causal child-start signal and verifies force-kill, reap,
  and timeout diagnostics.

The writer smoke test is not matrix evidence. The mutation-specific evidence is:

- fresh bootstrap/schema fingerprint: `TestOpenStoreBootstrapsSchema` and
  `TestOpenStoreBootstrapsPhase2Tables`;
- upsert and current-state side effects: `TestUpsertAndReadCurrentAlert`;
- transition insert: `TestInsertStateTransition`;
- poll and immutable transaction-local event payloads:
  `TestPollEventPayloadOrderingIsStableAcrossAlertsTargetsSourcesAndSilences` and
  `TestPollEventsAreOwnedByTheirTransactionWhenTimestampsMatch`;
- resolve: `TestResolveAlertSuccessFromInactive` and
  `TestResolveWriterPhaseUpdatesGlobalAndAllTargetRows`;
- housekeeping cleanup/retention/vacuum:
  `TestHousekeepingCleanupAndVacuumUseSeparateWriterTransactions` and
  `TestRunHousekeepingCleansCompletedUnreferencedJobs`, which proves deletion of an
  older unreferenced completed job while one completed job remains, plus
  `TestRunHousekeepingPreservesRulesOfLatestCompletedJob`, which proves latest-job
  immutable rule input retention, and
  `TestRunHousekeepingCascadesSupersededJobGraphAndPreservesLatestGraph`, which proves
  superseded rule/candidate cascade and latest-graph retention; direct inspection of
   `runPhase2RetentionTx` and schema-v1 foreign keys establishes global/target cause
   reference protection and attempts/operator-action cascade;
- Problem 10 configuration ownership: `CONFIGURATION.md` describes housekeeping as
  alert/limit/retention/vacuum only and points recheck execution to ADR-007; the
  cmd/karma scheduler remains the sole recheck owner and housekeeping is not a wake
  source;
- regroup begin/resume/batch/completion/failure:
  `TestRegroupBeginBatchAndCompletionUseRealWriterPhases`,
  `TestRegroupResumesAfterDelayedBeginCommitReconciliation`,
  `TestRegroupResumesAfterDelayedBatchCommitReconciliation`, and
  `TestRegroupFailureWritesErrorMetadataThroughWriter`;
- recheck batch/completion/failure: `TestRecheckZeroCandidateJobEmitsStartThenCompletion`,
  `TestRecheckFailurePersistenceIsSeparateWriterPhase`, and
  `TestRecheckCancellationBeforeLedgerInsertDoesNotCommit`.

Delayed-reconciliation and shutdown coverage additionally includes:

- `TestRegroupResumesAfterDelayedBeginCommitReconciliation` and
  `TestRegroupResumesAfterDelayedBatchCommitReconciliation`, which prove delayed
  commit handoff resumes the same regroup owner without rerunning a committed phase;
- `TestRecheckWriterInvariantAfterSelectionDoesNotFailJob` and the zero-candidate
  queued/running tests, which keep writer infrastructure errors out of job failure
  state and preserve start/resume before completion events;
- `TestPollEventsAreOwnedByTheirTransactionWhenTimestampsMatch`, which proves a
  delayed poll finalizer cannot replay another transaction's rows;
- `TestApplicationShutdownDrainsHTTPBeforeHistoryAndStore`,
  `TestApplicationShutdownTimeoutDoesNotCloseHistoryOrStore`, and
  `TestApplicationShutdownPropagatesStoreCloseError`, which cover the post-poll
  grace-period boundary and close-error propagation; and
- `TestHistoryPollerStartupReportsStopWinner`, which verifies startup readiness is
  an explicit result rather than a signal-only channel.

Problem 1 is implemented. The verification below
names PID ownership, exact regroup severity, deterministic ordering, and real-component
coverage. Problems 1-10 are implemented and independently accepted.

Additional writer/shutdown evidence includes:

- `TestPIDLeaseRejectsExistingPathWithoutChangingIt`,
  `TestPIDLeaseDoesNotFollowSymlinkOrDirectory`,
  `TestPIDLeaseAcquireWriteRelease`, replacement detection, partial-write cleanup,
  identity-retry cleanup, transient-release retry, close-error joining, and startup
  cleanup with an unowned pre-existing file;
- `TestPIDLeaseSharedReleasePrimitivesRetryIdentityAndPreserveState`,
  `TestPIDLeaseSharedReleasePrimitivesCurrentInspectionFailureJoinsClose`,
  `TestPIDLeaseSharedReleasePrimitivesOwnershipLossJoinsBothCloseErrors`,
  `TestPIDLeaseSharedReleasePrimitivesDeleteFailureRetainsLease`, and
  `TestPIDLeaseSharedReleasePrimitivesSuccessfulDeleteJoinsBothCloseErrors`, which
  execute the production-owned platform-neutral release state machine;
- `TestPIDLeaseSharedReleasePrimitivesUsesPhaseSpecificOwnershipClassification` and
  `TestPIDLeaseSharedReleasePrimitivesOpenCurrentOwnershipLossClosesCurrentAndOwned`,
  which prove identical raw errors retain the lease during retained identity capture,
  clear it only at current-path open, preserve it during non-ownership current identity
  reads, and cover both current-path ownership-loss forms. These branches also
  compare complete cleared snapshots with injected owned Close errors and explicit
  non-zero identities;
- `TestManagedFinalizeStatusPublishesExitAfterCleanup`,
  `TestManagedFinalizeStatusPersistsCleanupFailureBeforeExit`,
  `TestManagedFinalizeStatusFailsClosedWhenExitMarkerWriteFails`, and
  `TestManagedKarmaStartupTimeoutReapsLiveChildWithoutPID`, which prove managed
  finalization ordering, failure publication, fail-closed marker handling, child
  termination, and reap;
- `TestRegroupInfrastructureReasonSeverityUsesExactPairs`, the structured expected
  shutdown/external cancellation logs, and exact stage/reason validation;
- `TestRegroupOwnerStopJoinsWorkerWithoutElapsedOrderingProof` and concurrent-stop
  arbitration, plus real command-owner start/join tests;
- `TestStoreWorkersCancellationAtWriterBoundaryDoesNotPersistFailure` for regroup and
  housekeeping, `TestRecheckCancellationBeforeLedgerInsertDoesNotCommit`, and the
  transaction-local poll permutation test covering alerts, targets, sources, and
  silences without mutating caller input; and
- the native Windows PID lease test job in `.github/workflows/test.yml`, which accepts
  an explicit review SHA, prints and verifies the checked-out tree identity, and runs
  the Windows filesystem tests.

The accepted Problem 1 commit gate is reproducible with:

```sh
go test -race ./cmd/karma \
  -run TestManagedKarmaStartupTimeoutReapsLiveChildWithoutPID -count=100 -timeout 240s
go test -race ./cmd/karma \
  -run 'TestScripts/(096_tls_client|098_proxy_url_request)' \
  -count=5 -timeout 300s
go test -race ./internal/fork/alertlifecycle \
  -run 'TestWriterCommitRecoveryDoesNotUseCanceledOperationContext|TestRegroupWriterAcquisitionFailureDoesNotPersistDomainFailure|TestRecheckWriterAcquisitionFailureLeavesQueuedJobUntouched|TestWriterCancellationBeforeLedgerInsertWithoutHookDoesNotCommit' \
  -count=50 -timeout 240s
go test ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test ./cmd/karma -count=1 -timeout 240s
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test -race ./cmd/karma -count=1 -timeout 240s
go test ./... -count=1 -timeout 240s
make lint
git diff --check
GOOS=windows GOARCH=amd64 go test -c ./cmd/karma \
  -o /tmp/karma-cmd-windows.test.exe
rm -f /tmp/karma-cmd-windows.test.exe
```

The Windows command is local cross-compilation only; native Windows execution is not a
Problem 1 completion requirement.

Problem 6 process-infrastructure verification is reproducible with:

```sh
go test ./cmd/karma -run 'TestPIDLease(Unix|SharedReleasePrimitives)|TestManagedFinalizeStatus|TestManagedKarmaStartupTimeoutReapsLiveChildWithoutPID' -count=1 -timeout 240s
GOOS=windows GOARCH=amd64 go test -c ./cmd/karma -o /tmp/karma-cmd-windows.test.exe
rm -f /tmp/karma-cmd-windows.test.exe
```

The shared matrix is the Linux-runnable evidence gate for the platform-neutral release
decisions; the platform adapters remain covered by Windows cross-compilation and optional
native filesystem tests.

Problem 6 passed its independent acceptance and completion gate on 2026-08-24. The
focused command and Windows cross-compilation above passed, followed by the full Go
suite, both race suites run sequentially, and root `make lint`. The first lifecycle
race invocation exposed a non-reproduced existing cancellation-test signal; the exact
test passed 20 race repetitions and the complete lifecycle race gate then passed on a
fresh rerun. No Problem 6 behavior or acceptance assertion was changed in response.

Effective suppression explainability is covered by:

- `TestRunSilenceRecheckBatchPromotesInactiveAndAddsCause` for pinned-rule details;
- `TestListCurrentAlertsForMergeReturnsLatestLiveSuppressionCause` for a live cause
  retained after disappearance;
- `TestListCurrentAlertsForMergeReturnsUnresolvedCauseWithoutInventedDetails` for
  identity-only replication-lag evidence;
- `TestMergeLifecycleStoredAlertsRestoresSuppressionCauseForCard` for the backend
  alert API boundary;
- the retained-evidence case in
  `ui/src/Components/Grid/AlertGrid/AlertGroup/Alert/index.test.tsx` for read-only
  card rendering without Edit/Delete actions.

Historical alert source-link preservation is covered by:

- `TestBuildLiveLifecycleSnapshotCapturesAlertSourceLinks` for the live observation boundary;
- `TestLifecycleMergeRetainsSourceLinksAfterAlertDisappears` for SQLite persistence,
  non-live reconstruction, and absence of restored mutable silence/inhibition state.

Invalid silence entries are covered at every decision boundary:

- mapper: `TestSilencesSkipsInvalidEntriesIndependently`, duplicate-ID/field,
  invalid-UTF-8, unknown-extra-field, optional-field, and malformed-array tests;
- lifecycle current-set/history behavior: `TestUnknownSilenceStatusMeansConfirmedAbsence`;
- HA episode closure: `TestSilenceEpisodeClosesOnlyAfterConfirmedHAWideAbsence`.

These tests enforce the normative rule that an invalid individual silence is absent
from a successful source snapshot while valid neighbors continue. A structurally
malformed top-level array fails the complete refresh.

Problem 2 immutable recheck input is implemented. Its evidence includes:

- `TestProblem2SchemaContainsImmutableJobInputAndCombinedRetryTables`,
  `TestProblem2SchemaEnforcesInputConstraintsAndIndexes`,
  `TestProblem2SchemaIndexColumnOrderAndCompositeForeignKeys`,
  `TestProblem2SchemaChecksUniqueKeysAndForeignKeyActions`, and
  `TestProblem2SchemaAcceptsAllProblem2EnumAndTerminalValues`, which verify the fresh
  v1 columns, exact ordered indexes and composite foreign keys, all Problem 2 CHECKs,
  valid enum/terminal values, duplicate keys, isolated delete/update actions, and the
  absence of mutable current-state FKs;
- `TestProblem2EligibilitySuccessorContainsOnlyExactTargetScopes` and
  `TestRecheckCandidateBecomingEligibleCreatesImmutableSuccessor`, which prove that
  poll reconciliation creates a targeted successor and never appends or schedules a
  sibling target in the predecessor;
- `TestProblem2CanonicalJobFingerprintIgnoresInputOrderAndPreservesCaller`, which
  verifies deterministic canonical bytes and a fixed SHA-256 vector, complemented by
  `TestProblem2RuleFingerprintIgnoresLocalIDs` and
  `TestProblem2ProductionCanonicalizationIsStableAcrossFreshStores` plus
  `TestProblem2ProductionCanonicalizationCoversPermutedLiveTargets`;
- `TestProblem2GlobalObservationTokenRejectsNewLifecycleEpisode` and
  `TestProblem2TargetObservationTokenIsIndependent`, which prove separate global and
  target observation-token CAS boundaries;
- `TestProblem2DuplicateIntegrityMatrix`,
  `TestProblem2TargetDuplicateIntegrityMatrix`,
  `TestProblem2DuplicateIgnoresLegitimateRuntimeFields`, and the existing provenance
  conflict tests, which use one shared assertion path for immutable parent/rule/candidate
  conflicts, complete ordered parent/rule/candidate rows including runtime fields, exact
  counts, unsuccessful results, and zero queued events while excluding every current
  legitimate runtime field. `TestProblem2DuplicateRejectsOrderingOnlyPersistedByteDrift`
  and `TestProblem2DuplicateRejectsRevisionLinkageDriftWithinSameRule` cover ordering and
  same-rule revision boundaries. The ordering test creates its two-matcher, two-source,
  two-member input through the normal production creator, validates linked provenance,
  and asserts exact `rule row 0 differs`/`candidate row 0 differs` stages;
- `TestProblem2ApplyPollCycleCommittedJobEventAccounting`, which captures committed
  structured `recheck_job_queued` events from production `ApplyPollCycle` sequences and
  compares them with trigger counters and physical job-row deltas for creation,
  successor, and replay paths;
- `TestProblem2MissingCurrentAlertBecomesTerminalCandidateOutcome` and
  `TestProblem2WorkerRecordsOneTerminalOutcomePerCandidate`, which prove membership
  survives current-row eviction and every candidate receives one outcome with the
  counter-sum invariant;
- `TestProblem2CreatorDuplicateIsIdempotent`, which proves the healthy duplicate path
  does not append children, complemented by exact raw-byte, parent, child, rule-linkage,
  and linked-revision provenance conflict coverage; and
- `TestProblem2WorkerPrimaryGlobalCASZeroRowRollsBack`,
  `TestProblem2WorkerPrimaryTargetCASZeroRowRollsBack`,
  `TestProblem2WorkerClassifiesPrimaryPostCASStates`,
  `TestProblem2WorkerPostCASReadFailuresRollbackPrimaryAndAggregate`, and
  `TestProblem2WorkerAggregateCASZeroRowIsInvariant`, which prove worker-level
  rollback, primary/aggregate CAS invariants, classified stale/ineligible states, and
  post-CAS read-error propagation without committed transitions. Their snapshots compare
  complete alert/target state, suppression histories, members, aggregate links, and event
  rows byte-for-byte before and after rollback;
- `TestProblem2ForeignKeyActionsAreIsolatedToPinnedInput`, which proves parent-key,
  source-key, revision-key, and rule-key restrictions using candidate-only/rule-only jobs
  and non-current source/rule/revision fixtures with no unrelated source-silence edges;
- `TestProblem2WorkerCoversEveryTerminalOutcome` and
  `TestProblem2CompletePriorityBoundaryMatrix`, which cover every constrained terminal
  outcome, its exclusive parent counter, completion, and every adjacent priority
  boundary;
- `TestProblem2RestartResumesPinnedMembershipBetweenBatches` plus
  `TestC4RecheckResumesPinnedContextAndCandidatesAfterRestart`, which verify restart
  equality and resume from immutable membership, including immutable parent trigger and
  progress identity, local rule/revision linkage, terminal candidate fields, counters,
  and no replay of already-terminal candidates. The matrix also covers replay byte
  stability, max-alert eviction through worker completion, exact cluster-qualified
  source completeness, per-candidate accounting, and former-v1 layout rejection.

The independent acceptance gate passed on 2026-08-18 with these repository-root commands:

```sh
go test ./internal/fork/alertlifecycle -run 'TestProblem2|TestRecheckCandidateBecomingEligibleCreatesImmutableSuccessor|TestPinnedRecheckRuleCannotCrossSourceReassignment|TestC4RecheckResumesPinnedContextAndCandidatesAfterRestart|TestRecordedTrafficReplayThroughLifecycleStore' -count=1 -timeout 240s
go test ./... -count=1 -timeout 240s
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test -race ./cmd/karma -count=1 -timeout 240s
make lint
git diff --check
```

Problem 3 verification is organized by runtime capability.

**State machine, durable attempts, and retry history**

- `TestProblem3RetryDelay`, `TestProblem3QueueEligibility`, and
  `TestProblem3StateTransitions` cover the six-state machine, due-time FIFO eligibility,
  five retry delays, budget exhaustion, committed-progress reset, and operator terminal
  transitions.
- `TestProblem3CommittedBatchWritesAttemptLedger`,
  `TestProblem3TerminalFailurePersistsAttemptWithoutRetry`,
  `TestProblem3Review2ExactRetryScheduleAndErrorHistory`,
  `TestProblem3Review2IsolatedFailuresAcrossCommittedBatchesResetBudget`, and
  `TestProblem3Review2RunningJobResumesAfterRestart` cover committed/retry/failed attempt
  rows, exact counters and schedule, first/last error history, rollback before failure
  bookkeeping, and restart.
- `TestProblem3Review5SixthRetryableFailureLeavesLiveFailedHead` covers the exact fifth
  retry to sixth terminal-failure boundary while leaving the scheduler owner alive.

**Commit reconciliation and writer recovery**

- `TestProblem3Review2InternalReconciliationDeadlinePreservesAttempt`,
  `TestProblem3Review2InternalDeadlineResolvesPresentAndAbsent`,
  `TestProblem3Review2AttemptTupleTransientLookupRetries`,
  `TestProblem3Review2AttemptTupleTransientClassesRetryAtTransactionBoundary`,
  `TestProblem3Review2FailureBookkeepingTupleTransientRetriesWithoutSecondAttempt`, and
  `TestProblem3Review2CommittedRetryBookkeepingRestoresSchedule` cover generic-ledger plus
  full-attempt-tuple reconciliation without duplicate domain effects.
- `TestProblem3Review3InvalidGenericLookupConnectionIsReplaced`,
  `TestProblem3Review3InvalidAttemptTupleConnectionIsReplaced`, and
  `TestProblem3Review3ReplacementFailurePreservesPendingReconciliation` prove physical
  connection replacement and pending-state preservation after invalidation.
- `TestProblem3Review3GenericAbsentAttemptPresentIsInvariant` and
  `TestProblem3Review3RepresentativeTupleMismatchPreservesCompleteState` enforce
  dual-ledger agreement with complete no-effects snapshots.
- `TestProblem3Review6FailureReconciliationUnknownThenPresentThenUnknownUsesFreshBackoff`
  and the unknown/committed reset tests verify operation provenance, fresh reconciliation
  backoff, and no duplicate attempt.

**Single scheduler owner and timer semantics**

- `TestRecheckSchedulerWakeIsCapacityOne`,
  `TestRecheckSchedulerStartsOneOwnerAndStopsIdempotently`,
  `TestRecheckSchedulerProductionRunnerBackoffAndReset`,
  `TestRecheckSchedulerReplacesHeadTimerWithoutSleep`, and
  `TestRecheckSchedulerStartupAndCancellationOwnOneRunner` cover owner startup/shutdown,
  wake coalescing, acquisition backoff, persisted due-time replacement, and cancellation.
- `TestRecheckSchedulerLoopUsesIndependentBackoffs`,
  `TestRecheckSchedulerEachProcessOwnerHasOwnCappedSequence`,
  `TestRecheckSchedulerLoopCoalescesWakeWhileBatchActive`,
  `TestRecheckSchedulerLoopDeliversPersistedDueTimer`, and
  `TestCloseAlertLifecycleStoreJoinsSchedulerBeforeStoreClose` cover independent backoff
  owners, active-batch wake handling, timer delivery, and Store-close ordering.
- `TestProblem3Review3SchedulerUsesRealStoreReconciliation`,
  `TestProblem3Review3JoinedBookkeepingUnknownUsesRealStoreReconciliation`, and
  `TestProblem3Review3SchedulerCancellationJoinsActiveStoreTransaction` run the real Store
  boundary with deterministic causal barriers and no sleeps.
- `TestProblem3Review4WakeCannotBypassProcessLocalBackoff`,
  `TestProblem3Review4WakeCannotBypassPersistedRetryDueTimer`,
  `TestProblem3Review4EmptyStoreRunDoesNotSelfWake`,
  `TestProblem3Review4ReconciledHandoffWakesIdleSchedulerExactlyOnce`, and
  `TestProblem3Review5ImmediateReconciledEmptyRunDoesNotWake` cover timer ownership,
  no-spin empty work, and ordinary versus delayed reconciliation handoff.
- `TestProblem3Review3HousekeepingDoesNotWakeRecheckScheduler` and
  `TestProblem3Review3TerminalDispositionStopsSchedulerOwner` cover excluded ambient wake
  ownership and terminal infrastructure shutdown.

**Failure bookkeeping and acquisition classification**

- `TestProblem3Review5FailurePersistenceClassificationMatrix`,
  `TestProblem3Review5TerminalBookkeepingErrorsDoNotBackoff`, and the retained failure-log
  regression cover BUSY versus terminal classification, operation-token failure,
  unchanged state, and exactly-once `job_failed` emission.
- `TestProblem3Review6FailureBookkeepingAcquisitionIsRetryable`,
  `TestProblem3Review6FailureBookkeepingSQLConnDoneIsRetryable`, and
  `TestProblem3Review6TerminalCallbackWithUnavailableCleanupStaysTerminal` preserve the
  pending attempt across writer recovery without allowing cleanup unavailability to mask a
  terminal callback.
- `TestProblem3Review7FailureBookkeepingAcquisitionPrecedence`,
  `TestProblem3Review7SchedulerAcquisitionPrecedenceMatrix`,
  `TestProblem3Review8FirstReadinessReplacementCausePrecedenceAndNoEffects`, and
  `TestProblem3Review8SchedulerRawUnavailableFailsClosed` cover bad/closed connections,
  BUSY, FULL, corruption, unknown causes, and raw unavailable fail-closed behavior.
- `TestProblem3Review9RecoveryLinksRetainedAttemptAndCandidateLoop` links the retained
  pending token to the sole `retry_scheduled` row and proves one candidate-loop entry;
  `TestProblem3Review9SchedulerTypedUnavailableTerminalCausesStopWithoutTimer` proves the
  unavailable marker cannot override a terminal underlying cause.

**FIFO and operator recovery**

- `TestProblem3OperatorRetryAbandonAndReplayCAS`,
  `TestProblem3Review2CompleteFIFOAcrossRestartRetryAndAbandon`,
  `TestProblem3Review2OperatorReplaySurvivesRestartAndPreservesInput`, and
  `TestProblem3Review2AbandonReplaySurvivesRestartAndPreservesInput` cover same-job retry,
  abandonment, exact replay after restart, actor/payload conflicts, and FIFO release.
- `TestProblem3Review5FailedHeadRetryResumesSameJobThroughLiveScheduler`,
  `TestProblem3Review5FailedHeadAbandonReleasesLiveSchedulerSuccessor`,
  `TestProblem3Review6RetryPreservesImmutableInputAndFailureHistory`, and
  `TestProblem3Review6AbandonPreservesFailedHeadHistoryAndReleasesOnlySuccessor` cover live
  scheduler recovery plus immutable input, original-attempt, and first-error preservation.
- `TestRecheckSchedulerCommittedAbandonRunsQueuedSuccessor` proves committed abandonment
  immediately wakes the successor without a poll or unrelated timer.

**Schema and HTTP boundaries**

- `TestProblem3Review2CompleteSchemaRejectionMatrix` and the extended Problem 3 schema
  matrix cover status/timestamp, outcome, foreign-key, uniqueness, and accounting
  constraints with unchanged-state assertions.
- `TestProblem3Review3RegisteredRouterOperatorContract` and
  `TestProblem3Review4ApplicationRouterAuthenticationAndActorPropagation` cover actual
  route registration, configured prefix, strict JSON/body limits, 400/401/404/409 mapping,
  authenticated actor propagation, and post-commit wake.

Problem 8 production precedence is corrected. `problem8ExpectedFailureSnapshot` and
`problem8ExpectedCommittedSnapshot` now compare the complete jobs, candidates, attempts, events,
writer-ledger, operator-action, and pending-writer captures for cancellation-first,
retryable/terminal selected failures, confirmed-not-committed, unknown Commit, and reconciliation
phases; only operational timestamps and tokens are normalized. No independent-review finding
remains; the final completion gate passed.

Problem 9 production logging is panic-isolated. The registered writer-operation vocabulary and
focused evidence now include `recheck_operator_action`; `assertProblem9WriterEvent` counts the
complete registered `writer_observability_failed` records, requires exactly one, and rejects
additional lifecycle-writer or poll/recheck writer events while retaining whole-output redaction
assertions for its existing branches.

Problem 4 verification is organized by presentation capability. Authoritative 401 responses
invalidate the `AlertStore` authentication state, hide failed-head controls, and allow recovery
on a later successful response; network and 5xx failures do not log the operator out. Operator
replay-ledger semantics remain defined by ADR-005, ADR-007, `storage-schema.md`, and
`implementation-contracts.md`.

**Problem 5 end-to-end verification (implemented and independently accepted)**

The deterministic lifecycle sequence retains the required source-job → partial-batch →
later-successor order, stop/reopen barrier, registered-router phases, and integrated failed
recorded subset. Recorded-traffic cause evidence computes status and fingerprints from a
test-owned source/member template before any global or target history rows are read, and
both graphs are compared against that same independent expectation.

**Problems 5/8/9 historical completion gate**

The historical candidate passed from the repository root:

```bash
go test ./cmd/karma -run 'TestProblem5|TestRecordedTrafficReplayThroughLifecycleStore' -count=1 -timeout 240s
go test ./internal/fork/alertlifecycle -run 'TestProblem8|TestProblem9|TestWriterPanicCleanupPreservesPanicAndLogsRecoveryFailure' -count=1 -timeout 240s
go test ./cmd/karma -run '^TestProblem5CompleteDeterministicLifecycleSequence$' -count=20 -timeout 240s
go test -race ./cmd/karma -run '^(TestRecordedTrafficReplayThroughLifecycleStore|TestProblem5CompleteDeterministicLifecycleSequence)$' -count=1 -timeout 240s
go test ./... -count=1 -timeout 240s
make lint
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test -race ./cmd/karma -count=1 -timeout 240s
git diff --check
```

Final results: focused Problem 5 3.1s; focused Problems 8/9 0.57s; deterministic sequence 20/20;
focused Problem 5 race 51.4s; full Go 38.1s; root lint 0 issues; lifecycle race 134.9s; cmd race
132.6s; patch-integrity clean. Both race suites remained under the mandatory 240s timeout and ran
sequentially.

**Historical integration completion gate (2026-08-25)**

The then-current Problems 1-10 candidate passed:

```bash
go test ./internal/fork/alertlifecycle ./cmd/karma -run 'TestProblem5CompleteDeterministicLifecycleSequence|TestRecordedTrafficReplayThroughLifecycleStore|TestProblem4|TestProblem9' -count=1 -timeout 240s
npm --prefix ui test -- src/Components/ForkLifecycleWarning/index.test.tsx src/Hooks/useFetchGet.test.tsx src/Stores/AlertStore.test.ts
go test ./... -count=1 -timeout 240s
npm --prefix ui test -- --runInBand
npm --prefix ui run build
make lint
npm --prefix ui run test:visual -- --grep "Fork lifecycle warning"
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test -race ./cmd/karma -count=1 -timeout 240s
```

Results: focused Go passed; focused UI 111/111; full Go passed; UI 1205 passed
with 2 skipped; production build passed; lint reported 0 issues; approved Problem 4
browser matrix passed 18/18 on Chromium/Firefox/Android; lifecycle race 139.0s and cmd
race 175.0s, run strictly sequentially under the 240s timeout.

The repository-wide full visual corpus is not this approved silence-recheck gate. A
diagnostic run found unrelated legacy snapshot drift (for example `Grid.png` expected
4204px height and rendered 4196px); the scoped lifecycle-warning matrix remained green.

**Typed worker result and event ownership**

- Nil-error queue dispositions, transaction-owned snapshots, nullable queue depth,
  final-batch events, and selection-status capture are implemented. Delayed batch,
  retry, and terminal reconciliation return the retained pre-Commit typed payload even
  when the read pool is unavailable. Immediate and delayed retry/terminal failures use one
  after-unlock finalizer for Store-owned events and cleanup. Operator transitions publish a
  separate finalization barrier before writer release; worker pre-check and in-transaction
  checks prevent selection before `operator_action_applied` and its scheduler wake. An absent
  `recheck_batch` reconciliation transfers the retained pending record to
  `recheck_failure` before retrying bookkeeping, so the failure finalizer owns cleanup.
- The pre-selection/selected ownership split, acquisition-edge reset, terminal registry,
  safe optional job identity, and exact `job_state_conflict` category are implemented.
  Immediate/delayed barrier tests cover causal ordering, wakes, pending completion, replay,
  and subsequent Retry/Abandon progress. Exact event values, delayed operator-finalizer
  blocking, semantic replay equality, and the one-row operational writer-ledger delta are
  implemented. Expected semantic phases, all-field pending state, deterministic ledger
  token/kind/order, delayed pre-finalization records, complete replay event sets,
  no-new-attempt checks, and exact zero-offset UTC ledger bounds are present. The
  operator-triggered delayed reconciliation/finalizer/retry race is covered by the
  first/second `after_precheck` causal matrix.
- `TestProblem4PromotionEventsCarryCommittedTransitionIDs` and the deterministic multi-batch
  matrix cover committed transition IDs, exact immediate/delayed batch-local records,
  cumulative completion counters/duration, levels, ordering, and stable repeated deferral.

**Strict stats and warning APIs**

- Registered HTTP tests cover all six warning variants and all six job statuses with exact
  recursive values, nullability, types, field counts, and no extras. The malformed tuple matrix
  covers every missing, empty, whitespace, and invalid-time first/last member.
  `TestProblem4RegisteredRouterExactMatrix` covers both actions across authentication,
  marker, Fetch Metadata, media, body, not-found, state/request conflict, storage failure,
  success, and replay with exact response bodies. The success transition decodes all six tables,
  validates semantic and writer-ledger changes, runs reachable success/replay redaction, and
  compares every operator-action column. Exact zero-offset UTC action/ledger causal bounds are
  checked against the measured request interval without tolerance.

**Best-effort metadata and UI presentation**

- Transaction-local/post-commit seams, typed infrastructure precedence, safe success
  projections, poll redaction, and retained operator event ownership are implemented. Control
  and fault runs compare operation-owned state on the main paths.
   `TestProblem4FailureEventPanicMatchesControlAndRecovers` compares exact write sets and
  typed results for immediate/delayed `job_selected`/`job_failed` panics, then proves
  Retry/Abandon recovery and pending cleanup. The same matrix observes wakes before failure,
  compares failure/operator/progress event sequences, phase snapshots, final normalized
   writer-ledger rows, and real Abandon next-head progress. Controlled result timestamps are
   compared without normalization and each path requires the exact expected wake-phase sequence.
- Production request generations and safe error normalization are implemented. Tests start a
  stale JSON parse before advancing generation and prove its rejection cannot replace the new
  result. A stale retry timer cannot clear a newer pending generation's loading/retry state.
- Chromium, Firefox, and Android remain the scoped visual projects. Fixtures isolate and assert
  exact issue kinds and confirmation/cancel/loading/error/success/overflow states. Disabled
  controls differ by color, dashed border, and cursor while retaining semantic colors. Durable
  project-specific JSON snapshots record every simultaneously visible control. Interactive
  captures are measured before screenshots and focus states prove required contrast. The
  `focus-cancel-confirmation` and `focus-cancel-dismiss` states prove keyboard-visible focus.
  The `Fork lifecycle warning warning-only state`, `blocked-only state`, and `independent states
  are visually distinct` tests call `measureProblem4WarningState` before capture and persist
  state-specific JSON artifacts. Problem 4 visual evidence is closed.

**Problem 4 final completion gate (2026-08-21)**

```sh
go test ./internal/fork/alertlifecycle -run Problem4 -count=1 -timeout 240s
go test ./cmd/karma -run Problem4 -count=1 -timeout 240s
npm --prefix ui test -- --runInBand \
  src/Components/ForkLifecycleWarning/index.test.tsx \
  src/Components/MainModal/Configuration/ForkStorageStats.test.tsx \
  src/Hooks/useFetchGet.test.tsx \
  src/Stores/AlertStore.test.ts
npm --prefix ui run test:visual -- --grep "Fork lifecycle warning"
go test ./... -count=1 -timeout 240s
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test -race ./cmd/karma -count=1 -timeout 240s
npm --prefix ui test -- --runInBand
npm --prefix ui run build
make lint
git diff --check
```

All commands passed. The focused UI matrix ran 108 tests; the scoped Chromium, Firefox,
and Android visual matrix ran 18 tests; the full UI matrix ran 1200 tests with two intentional
skips and 78 snapshots; lint reported zero issues. The two race packages ran sequentially as
required by the shared testscript-resource contract. The Problem 4 authentication-retention
evidence is covered by the authoritative-401, non-authentication-failure,
warning-preservation, hidden-control, and recovery tests. No integration-review finding
remains; the final focused/UI gate is complete and independently accepted.

## Silence recheck completion verification

The accepted implementation is covered by these focused contracts:

 - `TestProblem4SchedulerEventPanicsDoNotChangeControlState` compares control and
  panicking-logger runs across retry scheduling, retry deferral, queue blocking, acquisition
  backoff, and terminal infrastructure paths, including the complete process result,
  timer/backoff/edge/runner snapshots. `TestProblem4RealRetrySchedulingSurvivesSchedulerEventPanic`
  repeats the retry branch through a real Store and compares the durable retry-wait control
  snapshot and exact persisted retry timer.
 - `TestProblem4RegisteredRouterRejectsUnchangedRequestID` exercises the registered
  application router and proves exact request IDs with leading/trailing whitespace and
  forbidden characters are rejected before Store callbacks, events, pending completion, or
  wakes.
 - The `ForkLifecycleWarning` component matrix drives the generation-owned A/B sequence
   through the component hook boundary: B succeeds with HTTP 200, stale A returns 401,
   and the confirmed issue text, `AlertStore` authentication, and Retry/Abandon controls
   remain governed by B. The test waits for A's fetch-mock pipeline to settle and flushes
   a subsequent event-loop turn inside `act` before asserting, so the post-A assertions
   cannot pass before the real stale response continuation runs; no mocked component
   generation-arbitration test is used as the acceptance proof.
 - `TestManagedProcessGroupUnexpectedProbeErrorFailsCleanup` and
  `TestManagedProcessGroupMarkerFollowsConfirmedKillAndReap` cover unexpected probe
  failure and real process-group cleanup/reap ordering. The latter waits for a readiness
  handshake from both TERM-resistant parent and descendant, records both TERM and SIGKILL,
  and counts the single primary `Wait`.
 - `TestProblem3StateTransitions` executes the existing state cases and calls
  `runCompleteRecheckStateEventMatrix`, which covers all 42 cells in the six-state/seven-event
  reducer matrix with complete expected snapshots for accepted and rejected transitions.
- `TestRecordedTrafficReplayThroughLifecycleStore` independently models promoted recorded
  source and target causes before reading result history, validates the complete pinned
  rule/revision semantics before binding IDs, enumerates exact direct-global, target, and
  global-aggregate sets, and compares source/job, status, fingerprint, timestamp, member,
  transition, nullable retirement, and target-aggregate rows. For the current all-target
  corpus, the exact cause cardinality contract is direct-global `== 0`, target `> 0`, and
  global-aggregate `> 0`; the test must not fabricate a target-less alert to produce a
  direct-global cause. Exact empty and non-empty sets reject extra histories, members, and
  transitions.

The recorded replay sorts scopes by `(alert_id, target_key)`, selects at least 80 scopes
with their original match labels, and uses batch size 20 so the source-triggered job spans
at least three real batches. It partitions scopes before processing into `promoted`, stale
observation, `already_resolved`, and `no_matching_rule` outcomes; runs one batch, restarts
the Store, and completes the remaining batches; and requires both `source_context_changed`
and disjoint `alert_became_eligible` jobs. A later promotion-capable successor job pins a
relevant source failed and requires `incomplete_source_coverage` for the fixed recorded
subset. The replay compares exact outcomes, counters, immutable membership, observation
tokens, fingerprints, suppression events, and cause revisions across restart, then checks
merge and preview output for both promoted and untouched recorded alerts.

 The focused closure command includes every new regression through the existing capability
 name prefixes:

```sh
go test ./internal/fork/alertlifecycle -run '^TestProblem3StateTransitions$' -count=20 -timeout 240s
go test ./cmd/karma -run 'TestProblem4|TestRecordedTrafficReplayThroughLifecycleStore|TestManaged' -count=1 -timeout 240s
npm --prefix ui test -- --runInBand \
  src/Hooks/useFetchGet.test.tsx \
  src/Components/ForkLifecycleWarning/index.test.tsx \
  src/Stores/AlertStore.test.ts
```

The focused backend and UI regressions pass, including the completion tests. The complete
repository candidate is independently accepted.

After all edits except the final evidence record, run the complete gate in this exact order:

```sh
go test ./internal/fork/alertlifecycle -count=1
go test ./cmd/karma -count=1
go test -race ./internal/fork/alertlifecycle -count=1 -timeout 240s
go test -race ./cmd/karma -count=1 -timeout 240s
npm --prefix ui test -- --runInBand \
  src/Components/ForkLifecycleWarning/index.test.tsx \
  src/Components/MainModal/Configuration/ForkStorageStats.test.tsx
npm --prefix ui run build
npm --prefix ui run test:visual -- --grep 'Fork lifecycle warning'
make lint
go test ./... -count=1 -timeout 240s
git diff --check
```

Run the race packages sequentially. Then run the complete UI suite and Windows
cross-compilation gate:

```sh
npm --prefix ui test -- --runInBand
GOOS=windows GOARCH=amd64 go test -c ./cmd/karma -o /tmp/karma-cmd-windows.test.exe
rm -f /tmp/karma-cmd-windows.test.exe
```

Run this exact Cyrillic scan from the repository root after the file changes; any match is
a failure:

```sh
if grep -RInP '[\x{0400}-\x{04FF}]' \
  --include='*.go' --include='*.ts' --include='*.tsx' \
  --include='*.js' --include='*.jsx' --include='*.md' \
  --include='*.yml' --include='*.yaml' --include='*.json' \
  --include='*.jsonc' --include='*.scss' --include='*.css' \
  --include='*.html' --exclude-dir=.git --exclude-dir=node_modules \
  --exclude-dir=dist .; then
  exit 1
fi
git diff --check
```

After appending the final evidence record, rerun this same Cyrillic scan and
`git diff --check`; the evidence record is the final documentation edit.

**Final completion gate results (2026-08-26)**

The independent final order passed: lifecycle package (`25.010s`), command package
(`40.532s`), lifecycle race (`145.733s`), command race (`172.405s`), focused UI (`28
passed`), production UI build, scoped warning visuals (`18 passed`), root lint (`0
issues`), and the full Go suite. The
additional complete UI suite passed (`1211 passed`, `2 skipped`, `78 snapshots`), and
`GOOS=windows GOARCH=amd64 go test -c ./cmd/karma` passed. The exact focused
commands also passed: the 42-cell state matrix ran with `-count=20`, the command matrix
included the `TestProblem4` and `TestManaged` capability tests, and the UI matrix included
the real `useFetchGet`/`ForkLifecycleWarning` boundary, fetch, warning, and AlertStore
suites (`117 passed`). The final Cyrillic scan, conflict scan, and `git diff --check`
passed. Problems 1-10 and the final stabilization findings are independently accepted.

## Runtime logging

```sh
go test ./internal/log
go test ./internal/alertmanager
go test ./internal/fork/alertlifecycle
go test ./cmd/karma -count=1 -timeout 240s
cd ui
npm test -- --runInBand \
  src/Components/SilenceModal/SilenceEditor.test.tsx \
  src/Components/Toast/AppToasts.test.tsx
```

Coverage includes mandatory component/event fields, level filtering, lifecycle and
silence transitions, source failure/recovery, HA disagreement/convergence, recheck
execution, Resolve, preview/cache, server-routed outcomes, browser-routed correlated
console events, and sensitive-data redaction. The normative event inventory and
field rules are in `runtime-logging-contract.md`.

## Canonical matcher parity

```sh
cd tools/matcher-corpus
go run . ../../fork-docs/SPEC/testdata/alertmanager-matcher-corpus.json
cd ../../ui
npm test -- SilenceEditor.matcher-corpus.test.ts --runInBand
```

The isolated tool pins Alertmanager v0.33.1 without adding it to Karma's runtime
dependency graph.

## Timezone contract

```sh
cd ui
TZ=America/New_York npm test -- SilenceEditor.timezone.test.ts --runInBand
```

This explicitly verifies rejection of a nonexistent spring-forward wall time and the
earlier offset for an ambiguous fall-back wall time.

## Performance budgets

```sh
go test ./internal/fork/alertlifecycle -run '^$' \
  -bench BenchmarkTargetLabelValueStats100k -benchtime=1x -benchmem
go test ./cmd/karma -run '^$' \
  -bench BenchmarkSilenceEditorPreview100k -benchtime=1x -benchmem
```

Reference run on Intel Core Ultra 5 125H:

- target label values, 100k rows: ~54 ms indexed; ~133 ms with one stale live
  alert overlay, both ~8-16 KB allocated in measured query;
- silence preview, 100k candidates/50k matches: ~54 ms, ~44 MB allocated.

The preview cache enforces a 100k-record per-result cap; broader requests fail with
HTTP 422 rather than exceeding the memory budget.

## UI and browser

```sh
cd ui
make lint
npm test -- --runInBand
npm run build
```

Run Playwright in the official image with host user mapping and never concurrently
with host npm commands against the same bind-mounted `ui/node_modules`.

## Production build boundary

`npm run build` uses `ui/tsconfig.build.json`, rooted at `src/index.tsx`, so test,
e2e, fixture, and matcher-corpus files are not production build dependencies. Root
`make lint` uses the full `ui/tsconfig.json` and still type-checks those files.

The Docker Node stage therefore copies only `ui/`; it does not copy
`fork-docs/SPEC/testdata` into the production build context.

## Release gate

Before release, run the current commands rather than copying historical test counts:

```sh
make lint
go test ./... -count=1 -timeout 240s
npm --prefix ui test -- --runInBand
npm --prefix ui run build
git diff --check
```

During development, a focused Jest command may be run before the full UI suite, for
example `npm --prefix ui test -- --runInBand path/to/file.test.tsx`. It does not
replace the full release command above.

For compact-editor layout/interaction changes, also run the official Playwright
image across Chromium, Firefox, WebKit, iPhone, and Android. Matcher, timezone,
recorded replay, and 100k performance commands above remain specialized gates for
their corresponding contracts.

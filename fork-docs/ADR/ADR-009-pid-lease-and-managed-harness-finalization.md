# ADR-009: PID Lease Recovery and Managed Harness Finalization

- Status: Implemented and independently accepted
- Date: 2026-08-24

## Context

The PID file is process ownership state, not a removable path owned by startup
cleanup. Identity capture can fail temporarily, and a path can be replaced while
the original handle remains open. The managed test wrapper also has to report a
child result only after it has finished termination cleanup; otherwise a waiter can
observe a successful exit before a cleanup failure is recorded.

## Decision

1. A PID lease keeps its opened file handle and platform identity while it owns the
   path. A transient identity failure preserves ownership and removes nothing.
2. A later release retries identity capture. Removal is allowed only after the
   current path proves to be the same file. Replacement or ownership loss clears
   the lease without deleting the replacement path.
3. Unix and Windows release paths join all relevant temporary/current/owned handle
   close errors with the primary result.
   The platform adapters supply only identity, current-handle, delete, and close
   primitives to the shared release state machine.
   Ownership loss is explicitly classified at the phase that proved it: retained
   identity capture, current-path open, or current-handle identity inspection.
   A privileged replacement after the final current-handle identity comparison but
   before path deletion is outside the local single-operator threat model.
4. The managed wrapper performs process-group cleanup before publishing `exit`.
   Cleanup failures are atomically written to `harness-error` before `exit` is
   published. Exit-marker failures are persisted as harness failures and return
   the protocol failure code.
   A process-group probe error other than confirmed `ESRCH` is a cleanup failure;
   it must never be treated as proof that the group disappeared.
   The force-kill verification fixture starts cleanup only after a causal readiness
   handshake confirms both the TERM-resistant parent and descendant are installed;
   it records TERM, SIGKILL, confirmed group disappearance, and exactly one primary
   child reap.
5. These rules are test-harness and process-ownership infrastructure only. They do
   not change lifecycle job state, retry policy, schema, or application APIs.

## Verification

- `TestPIDLeaseUnixReleaseRetriesTransientIdentityCapture`
- `TestPIDLeaseUnixReleaseJoinsOwnedCloseError`
- `TestPIDLeaseUnixOwnershipLossJoinsOwnedCloseError`
- `TestPIDLeaseSharedReleasePrimitivesRetryIdentityAndPreserveState`
- `TestPIDLeaseSharedReleasePrimitivesCurrentInspectionFailureJoinsClose`
- `TestPIDLeaseSharedReleasePrimitivesOwnershipLossJoinsBothCloseErrors`
- `TestPIDLeaseSharedReleasePrimitivesDeleteFailureRetainsLease`
- `TestPIDLeaseSharedReleasePrimitivesSuccessfulDeleteJoinsBothCloseErrors`
- `TestPIDLeaseSharedReleasePrimitivesUsesPhaseSpecificOwnershipClassification`
- `TestPIDLeaseSharedReleasePrimitivesOpenCurrentOwnershipLossClosesCurrentAndOwned`
- `TestManagedFinalizeStatusPublishesExitAfterCleanup`
- `TestManagedFinalizeStatusPersistsCleanupFailureBeforeExit`
- `TestManagedFinalizeStatusFailsClosedWhenExitMarkerWriteFails`
- `TestManagedKarmaStartupTimeoutReapsLiveChildWithoutPID`
- Windows PID lease cross-compilation and native Windows filesystem tests
- `TestManagedProcessGroupUnexpectedProbeErrorFailsCleanup`
- `TestManagedProcessGroupMarkerFollowsConfirmedKillAndReap`

package alertlifecycle

import (
	"context"
	"database/sql"
	"time"
)

// RecheckIntegrationTestHooks exposes transaction-boundary controls only for repository
// integration tests that must run the cmd-owned scheduler against a real Store.
type RecheckIntegrationTestHooks struct {
	Commit                   func(func() error) error
	WriterCommitLookup       func(context.Context, string, string) (bool, error)
	AttemptLookup            func(context.Context, string) (int64, int, string, error)
	TransactionFinished      func(string)
	RecheckFault             func(string) error
	BeforeCommitLedgerInsert func(context.Context, string)
	Now                      func() time.Time
	WorkerBarrier            func(string)
	OperatorFinalizer        func()
	FailureFinalizer         func()
	OperationToken           func() (string, error)
}

// RecheckPendingStateSnapshot is a test-only copy of process-local recheck handoff state.
// It is intentionally not used by runtime code or exposed through an application API.
type RecheckPendingStateSnapshot struct {
	FailurePresent              bool
	FailureJobID                int64
	FailureAttemptToken         string
	FailureAttemptNumber        int
	FailureOperation            string
	FailureResultReady          bool
	FailureStatus               string
	FailureDisposition          RecheckBatchDisposition
	FailureCompletionCommitted  bool
	FailureCompletionFinalized  bool
	OperatorPresent             bool
	OperatorJobID               int64
	OperatorRequestID           string
	OperatorCompletionCommitted bool
	OperatorCompletionFinalized bool
}

// RecheckPendingStateSnapshot returns process-local state for no-effects integration tests.
func (s *Store) RecheckPendingStateSnapshot() RecheckPendingStateSnapshot {
	if s == nil {
		return RecheckPendingStateSnapshot{}
	}
	s.recheckStateMu.Lock()
	defer s.recheckStateMu.Unlock()
	snapshot := RecheckPendingStateSnapshot{}
	if pending := s.pendingRecheckFailure; pending != nil {
		snapshot.FailurePresent = true
		snapshot.FailureJobID = pending.Attempt.JobID
		snapshot.FailureAttemptToken = pending.Attempt.Token
		snapshot.FailureAttemptNumber = pending.Attempt.Number
		snapshot.FailureOperation = pending.Operation
		snapshot.FailureResultReady = pending.ResultReady
		snapshot.FailureStatus = pending.Result.Status
		snapshot.FailureDisposition = pending.Result.Disposition
		if pending.Completion != nil {
			snapshot.FailureCompletionCommitted = pending.Completion.isCommitted()
			snapshot.FailureCompletionFinalized = pending.Completion.isFinalized()
		}
	}
	if pending := s.pendingRecheckOperator; pending != nil {
		snapshot.OperatorPresent = true
		snapshot.OperatorJobID = pending.JobID
		snapshot.OperatorRequestID = pending.RequestID
		if pending.Completion != nil {
			snapshot.OperatorCompletionCommitted = pending.Completion.isCommitted()
			snapshot.OperatorCompletionFinalized = pending.Completion.isFinalized()
		}
	}
	return snapshot
}

// SetRecheckIntegrationTestHooks installs or clears the narrow recheck integration-test
// controls. Runtime construction never calls this method.
func (s *Store) SetRecheckIntegrationTestHooks(hooks *RecheckIntegrationTestHooks) {
	if s == nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if hooks == nil {
		s.writerCommit = nil
		s.writerCommitLookup = nil
		s.recheckAttemptLookup = nil
		s.writerTransactionFinished = nil
		s.beforeCommitLedgerInsert = nil
		s.writerOperationToken = nil
		if s.lifecycleTestHooks != nil {
			s.lifecycleTestHooks.mu.Lock()
			s.lifecycleTestHooks.recheckFault = nil
			s.lifecycleTestHooks.recheckNow = nil
			s.lifecycleTestHooks.recheckWorkerBarrier = nil
			s.lifecycleTestHooks.recheckOperatorFinalizer = nil
			s.lifecycleTestHooks.recheckFailureFinalizer = nil
			s.lifecycleTestHooks.mu.Unlock()
		}
		return
	}
	if hooks.Commit == nil {
		s.writerCommit = nil
	} else {
		s.writerCommit = func(tx *sql.Tx) error { return hooks.Commit(tx.Commit) }
	}
	s.writerCommitLookup = hooks.WriterCommitLookup
	s.recheckAttemptLookup = hooks.AttemptLookup
	s.writerTransactionFinished = hooks.TransactionFinished
	s.beforeCommitLedgerInsert = hooks.BeforeCommitLedgerInsert
	s.writerOperationToken = hooks.OperationToken
	if s.lifecycleTestHooks == nil {
		s.lifecycleTestHooks = &storeLifecycleTestHooks{}
	}
	s.lifecycleTestHooks.mu.Lock()
	s.lifecycleTestHooks.recheckFault = hooks.RecheckFault
	s.lifecycleTestHooks.recheckNow = hooks.Now
	s.lifecycleTestHooks.recheckWorkerBarrier = hooks.WorkerBarrier
	s.lifecycleTestHooks.recheckOperatorFinalizer = hooks.OperatorFinalizer
	s.lifecycleTestHooks.recheckFailureFinalizer = hooks.FailureFinalizer
	s.lifecycleTestHooks.mu.Unlock()
}

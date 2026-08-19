package alertlifecycle

import (
	"context"
	"database/sql"
)

// RecheckIntegrationTestHooks exposes transaction-boundary controls only for repository
// integration tests that must run the cmd-owned scheduler against a real Store.
type RecheckIntegrationTestHooks struct {
	Commit              func(func() error) error
	WriterCommitLookup  func(context.Context, string, string) (bool, error)
	AttemptLookup       func(context.Context, string) (int64, int, string, error)
	TransactionFinished func(string)
	RecheckFault        func(string) error
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
		if s.lifecycleTestHooks != nil {
			s.lifecycleTestHooks.mu.Lock()
			s.lifecycleTestHooks.recheckFault = nil
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
	if s.lifecycleTestHooks == nil {
		s.lifecycleTestHooks = &storeLifecycleTestHooks{}
	}
	s.lifecycleTestHooks.mu.Lock()
	s.lifecycleTestHooks.recheckFault = hooks.RecheckFault
	s.lifecycleTestHooks.mu.Unlock()
}

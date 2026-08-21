package alertlifecycle

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

var ErrRecheckInfrastructureContract = errors.New("recheck infrastructure contract violation")

var errRecheckOperatorFinalizing = errors.New("recheck operator action is finalizing")

type recheckFailureCompletion struct {
	mu            sync.Mutex
	committedDone chan struct{}
	finalizedDone chan struct{}
	committed     bool
	finalized     bool
}

func newRecheckFailureCompletion() *recheckFailureCompletion {
	return &recheckFailureCompletion{committedDone: make(chan struct{}), finalizedDone: make(chan struct{})}
}

func (c *recheckFailureCompletion) completeCommitted() {
	c.mu.Lock()
	if !c.committed {
		c.committed = true
		close(c.committedDone)
	}
	c.mu.Unlock()
}

func (c *recheckFailureCompletion) isCommitted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.committed
}

func (c *recheckFailureCompletion) completeFinalized() {
	c.mu.Lock()
	if !c.finalized {
		c.finalized = true
		close(c.finalizedDone)
	}
	c.mu.Unlock()
}

func (c *recheckFailureCompletion) isFinalized() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finalized
}

func (c *recheckFailureCompletion) waitFinalized(ctx context.Context) error {
	select {
	case <-c.finalizedDone:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func recheckSelectedInfrastructureReason(ctx context.Context, err error) string {
	var acquisitionErr *writerAcquisitionError
	if errors.As(err, &acquisitionErr) {
		return "writer_unavailable"
	}
	if errors.Is(err, ErrWriterOperationInvariant) {
		return "writer_invariant"
	}
	var outcome *writerCommitOutcomeError
	if errors.As(err, &outcome) {
		// Commit outcomes are handled by the worker's reconciliation/failure
		// path, never by the terminal infrastructure taxonomy.
		return ""
	}
	if errors.Is(err, errWriterRollbackFailed) {
		return "rollback_failed"
	}
	if errors.Is(err, ErrWriterUnavailable) {
		return "writer_unavailable"
	}
	if errors.Is(err, ErrStoreClosing) {
		return "store_closing"
	}
	if errors.Is(err, ErrStoreClosed) {
		return "store_closed"
	}
	if ctx != nil && errors.Is(err, context.Canceled) && (errors.Is(ctx.Err(), context.Canceled) || errors.Is(context.Cause(ctx), context.Canceled)) {
		return "worker_cancelled"
	}
	if ctx != nil && errors.Is(err, context.DeadlineExceeded) && (errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(context.Cause(ctx), context.DeadlineExceeded)) {
		return "worker_deadline"
	}
	return ""
}

func ValidRecheckInfrastructurePair(stage, reason string) bool {
	switch stage {
	case "batch":
		switch reason {
		case "writer_unavailable", "writer_invariant", "rollback_failed", "store_closing", "store_closed", "worker_cancelled", "worker_deadline", "infrastructure_unclassified", "queue_read_terminal":
			return true
		}
	case "failure_persistence":
		switch reason {
		case "failure_persistence_sql_error", "failure_persistence_rollback_failed", "failure_persistence_writer_invariant", "failure_persistence_writer_unavailable", "failure_persistence_store_closing", "failure_persistence_store_closed", "failure_persistence_cancelled", "failure_persistence_deadline":
			return true
		}
	}
	return false
}

// RecheckFailurePersistenceReason is the single terminal reason classifier for
// failure-bookkeeping infrastructure errors. Commit outcomes are deliberately
// excluded: the scheduler routes those through reconciliation first.
func RecheckFailurePersistenceReason(err error) string {
	switch {
	case errors.Is(err, errWriterRollbackFailed):
		return "failure_persistence_rollback_failed"
	case errors.Is(err, ErrWriterOperationInvariant), errors.Is(err, ErrWriterOperationToken):
		return "failure_persistence_writer_invariant"
	case errors.Is(err, ErrWriterUnavailable), IsWriterAcquisitionError(err):
		return "failure_persistence_writer_unavailable"
	case errors.Is(err, ErrStoreClosing):
		return "failure_persistence_store_closing"
	case errors.Is(err, ErrStoreClosed):
		return "failure_persistence_store_closed"
	case errors.Is(err, context.Canceled):
		return "failure_persistence_cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "failure_persistence_deadline"
	default:
		return "failure_persistence_sql_error"
	}
}

func RecheckInfrastructureLogLevel(stage, reason string) slog.Level {
	if stage == "batch" && (reason == "worker_cancelled" || reason == "worker_deadline" || reason == "store_closing" || reason == "store_closed") {
		return slog.LevelDebug
	}
	if stage == "failure_persistence" && (reason == "failure_persistence_store_closing" || reason == "failure_persistence_store_closed" || reason == "failure_persistence_cancelled" || reason == "failure_persistence_deadline") {
		return slog.LevelDebug
	}
	return slog.LevelError
}

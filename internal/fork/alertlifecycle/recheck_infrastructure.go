package alertlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
)

var ErrRecheckInfrastructureContract = errors.New("recheck infrastructure contract violation")

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
		switch outcome.outcome {
		case writerCommitOutcomeUnknown:
			return "commit_outcome_unknown"
		case writerCommitOutcomeNotCommitted:
			return "commit_not_committed"
		case writerCommitOutcomeCommitted:
			return ""
		}
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

func emitRecheckInfrastructureBlocked(ctx context.Context, jobID int64, writerOperation, reason string, err error) error {
	stage := "batch"
	if writerOperation == writerOperationRecheckFailure {
		stage = "failure_persistence"
	}
	level := recheckInfrastructureLogLevel(stage, reason)
	if !validRecheckInfrastructurePair(stage, reason) {
		runtimeLog.ErrorContext(ctx, "silence_recheck", "job_infrastructure_contract_violation", "Invalid silence recheck infrastructure contract", slog.Int64("job_id", jobID), slog.String("writer_operation", writerOperation), slog.String("supplied_stage", stage), slog.String("supplied_reason", reason), slog.Any("error", errors.Join(ErrWriterOperationInvariant, ErrRecheckInfrastructureContract, err)))
		return errors.Join(ErrWriterOperationInvariant, ErrRecheckInfrastructureContract)
	}
	runtimeLog.Event(ctx, level, "silence_recheck", "job_infrastructure_blocked", "Silence recheck infrastructure blocked job processing", slog.Int64("job_id", jobID), slog.String("writer_operation", writerOperation), slog.String("stage", stage), slog.String("reason_code", reason), slog.Any("error", err))
	return nil
}

func validRecheckInfrastructurePair(stage, reason string) bool {
	switch stage {
	case "batch":
		switch reason {
		case "commit_not_committed", "writer_invariant", "rollback_failed", "worker_cancelled", "worker_deadline":
			return true
		}
	case "failure_persistence":
		switch reason {
		case "failure_persistence_commit_unknown", "failure_persistence_not_committed", "failure_persistence_sql_error", "failure_persistence_rollback_failed", "failure_persistence_writer_invariant", "failure_persistence_writer_unavailable", "failure_persistence_store_closing", "failure_persistence_store_closed", "failure_persistence_cancelled", "failure_persistence_deadline":
			return true
		}
	}
	return false
}

func recheckInfrastructureLogLevel(stage, reason string) slog.Level {
	if stage == "batch" && (reason == "worker_cancelled" || reason == "worker_deadline") {
		return slog.LevelDebug
	}
	if stage == "failure_persistence" && (reason == "failure_persistence_store_closing" || reason == "failure_persistence_store_closed" || reason == "failure_persistence_cancelled" || reason == "failure_persistence_deadline") {
		return slog.LevelDebug
	}
	return slog.LevelError
}

func recheckFailurePersistenceReason(reason string) string {
	switch reason {
	case "commit_outcome_unknown":
		return "failure_persistence_commit_unknown"
	case "commit_not_committed":
		return "failure_persistence_not_committed"
	case "rollback_failed":
		return "failure_persistence_rollback_failed"
	case "writer_invariant":
		return "failure_persistence_writer_invariant"
	case "writer_unavailable":
		return "failure_persistence_writer_unavailable"
	case "store_closing":
		return "failure_persistence_store_closing"
	case "store_closed":
		return "failure_persistence_store_closed"
	case "worker_cancelled":
		return "failure_persistence_cancelled"
	case "worker_deadline":
		return "failure_persistence_deadline"
	default:
		return "failure_persistence_sql_error"
	}
}

func (s *Store) persistRecheckFailure(ctx context.Context, jobID int64, domainErr error, startedAt time.Time) error {
	completion := newRecheckFailureCompletion()
	finalizer := &writerAfterCommitFinalizer{
		ctx:           ctx,
		operationKind: writerOperationRecheckFailure,
		handoff: func(bool) {
			completion.completeCommitted()
		},
		onHandoffError: func(err error) {
			_ = err
			completion.completeCommitted()
			completion.completeFinalized()
		},
		run: func(bool) {
			if completion.isCommitted() {
				runtimeLog.ErrorContext(ctx, "silence_recheck", "job_failed", "Silence recheck job failed", slog.Int64("job_id", jobID), slog.String("stage", "worker"), slog.Any("error", domainErr), slog.Duration("duration", time.Since(startedAt)))
			}
			completion.completeFinalized()
		},
	}

	for {
		_, persistErr := withWriterTxValueAfterCommit(ctx, s, writerOperationRecheckFailure, func(tx *sql.Tx) (struct{}, error) {
			nowRaw := time.Now().UTC().Format(time.RFC3339Nano)
			_, err := tx.Exec(`
UPDATE silence_recheck_jobs
SET status='failed', last_error=COALESCE(last_error, ?), finished_at=COALESCE(finished_at, ?)
WHERE job_id=? AND status IN ('queued','running','failed')
`, domainErr.Error(), nowRaw, jobID)
			return struct{}{}, err
		}, finalizer)
		if persistErr == nil {
			return domainErr
		}
		if errors.Is(persistErr, ErrWriterCommitReconciled) {
			if completion.isCommitted() {
				return domainErr
			}
			continue
		}
		reason := recheckSelectedInfrastructureReason(ctx, persistErr)
		if reason == "commit_outcome_unknown" {
			if contractErr := emitRecheckInfrastructureBlocked(ctx, jobID, writerOperationRecheckFailure, "failure_persistence_commit_unknown", persistErr); contractErr != nil {
				return errors.Join(domainErr, persistErr, contractErr)
			}
			return errors.Join(domainErr, persistErr)
		}
		failureReason := recheckFailurePersistenceReason(reason)
		if contractErr := emitRecheckInfrastructureBlocked(ctx, jobID, writerOperationRecheckFailure, failureReason, persistErr); contractErr != nil {
			return errors.Join(domainErr, persistErr, contractErr)
		}
		return errors.Join(domainErr, persistErr)
	}
}

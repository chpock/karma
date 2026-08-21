package alertlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
)

// RecheckBatchDisposition is the committed-state classification returned by a
// worker invocation. It is distinct from the job state machine and does not
// introduce another durable job status.
type RecheckBatchDisposition string

const (
	RecheckBatchQueueEmpty               RecheckBatchDisposition = "queue_empty"
	RecheckBatchCommitted                RecheckBatchDisposition = "batch_committed"
	RecheckBatchRetryScheduled           RecheckBatchDisposition = "retry_scheduled"
	RecheckBatchRetryDeferred            RecheckBatchDisposition = "retry_deferred"
	RecheckBatchQueueBlocked             RecheckBatchDisposition = "queue_blocked"
	RecheckBatchAcquisitionFailed        RecheckBatchDisposition = "acquisition_failed"
	RecheckBatchInfrastructureFailed     RecheckBatchDisposition = "infrastructure_failed"
	RecheckBatchCommitOutcomeUnknown     RecheckBatchDisposition = "commit_outcome_unknown"
	RecheckBatchJobFailed                RecheckBatchDisposition = "job_failed"
	RecheckBatchFailurePersistenceFailed RecheckBatchDisposition = "failure_persistence_failed"
)

type RecheckErrorSnapshot struct {
	Class   string
	Stage   string
	At      time.Time
	Message string
}

type RecheckOutcomeCounts struct {
	Promoted          int
	Stale             int
	Ineligible        int
	MissingLabels     int
	IncompleteSources int
}

type RecheckHeadJob struct {
	JobID                 int64
	TriggerType           string
	Status                string
	CreatedAt             time.Time
	StartedAt             *time.Time
	FinishedAt            *time.Time
	TotalCandidates       int
	ProcessedCandidates   int
	OutcomeCounts         RecheckOutcomeCounts
	AttemptCount          int
	RetryCount            int
	ConsecutiveRetryCount int
	NextAttemptAt         *time.Time
	FailureClass          string
	FailureStage          string
	FirstError            *RecheckErrorSnapshot
	LastError             *RecheckErrorSnapshot
	RecoveryCount         int
}

type RecheckStats struct {
	QueueDepth int
	HeadJob    *RecheckHeadJob
}

// RecheckBatchResult contains only metadata established by the worker's
// committed transaction or by a committed queue-head read. The accompanying
// error remains the operational cause used by the scheduler.
type RecheckBatchResult struct {
	Disposition RecheckBatchDisposition
	// ReconciledOperation identifies a durable recheck writer operation that
	// produced this result during delayed commit reconciliation.
	ReconciledOperation   string
	JobID                 int64
	TriggerType           string
	Status                string
	QueueDepth            *int
	CreatedAt             *time.Time
	StartedAt             *time.Time
	FinishedAt            *time.Time
	TotalCandidates       int
	ProcessedCandidates   int
	OutcomeCounts         RecheckOutcomeCounts
	AttemptCount          int
	RetryCount            int
	ConsecutiveRetryCount int
	NextAttemptAt         *time.Time
	FailureClass          string
	FailureStage          string
	FirstError            *RecheckErrorSnapshot
	LastError             *RecheckErrorSnapshot
	RecoveryCount         int
}

func readRecheckJobResultTx(ctx context.Context, tx *sql.Tx, jobID int64, strictErrorTuple bool) (RecheckBatchResult, error) {
	var (
		result                                                                 RecheckBatchResult
		triggerType, status                                                    string
		createdAtRaw, startedAtRaw, finishedAtRaw, nextAttemptAtRaw            sql.NullString
		firstErrorMessage, firstErrorClass, firstErrorStage, firstErrorAt      sql.NullString
		lastErrorMessage, lastErrorClass, lastErrorStage, lastErrorAt          sql.NullString
		jobIDRead, total, processed, promoted, ineligible, missing, incomplete int64
		stale, attempts, retries, consecutive, recovery, queueDepth            int64
	)
	err := tx.QueryRowContext(ctx, `
SELECT job_id, trigger_type, status, created_at, started_at, finished_at,
       total_candidates, processed_candidates, promoted_candidates,
       skipped_ineligible, skipped_missing_labels, skipped_incomplete_sources,
       stale_conflicts, attempt_count, retry_count, consecutive_retry_count,
       next_attempt_at, first_error_message, first_error_class, first_error_stage,
       first_error_at, last_error_message, last_error_class, last_error_stage,
       last_error_at, recovery_count
FROM silence_recheck_jobs
WHERE job_id=?`, jobID).Scan(
		&jobIDRead, &triggerType, &status, &createdAtRaw, &startedAtRaw, &finishedAtRaw,
		&total, &processed, &promoted, &ineligible, &missing, &incomplete, &stale,
		&attempts, &retries, &consecutive, &nextAttemptAtRaw, &firstErrorMessage,
		&firstErrorClass, &firstErrorStage, &firstErrorAt, &lastErrorMessage,
		&lastErrorClass, &lastErrorStage, &lastErrorAt, &recovery,
	)
	if err != nil {
		return RecheckBatchResult{}, fmt.Errorf("failed to read durable recheck result: %w", err)
	}
	createdAt, err := parseRecheckStatsTime(createdAtRaw, "created_at")
	if err != nil || createdAt == nil {
		if err == nil {
			err = errors.New("created_at is NULL")
		}
		return RecheckBatchResult{}, err
	}
	startedAt, err := parseRecheckStatsTime(startedAtRaw, "started_at")
	if err != nil {
		return RecheckBatchResult{}, err
	}
	finishedAt, err := parseRecheckStatsTime(finishedAtRaw, "finished_at")
	if err != nil {
		return RecheckBatchResult{}, err
	}
	nextAttemptAt, err := parseRecheckStatsTime(nextAttemptAtRaw, "next_attempt_at")
	if err != nil {
		return RecheckBatchResult{}, err
	}
	firstError, err := buildRecheckStatsError(firstErrorMessage, firstErrorClass, firstErrorStage, firstErrorAt)
	invalidErrorTuple := false
	if err != nil {
		if strictErrorTuple {
			return RecheckBatchResult{}, err
		}
		invalidErrorTuple = true
		firstError = nil
	}
	lastError, err := buildRecheckStatsError(lastErrorMessage, lastErrorClass, lastErrorStage, lastErrorAt)
	if err != nil {
		if strictErrorTuple {
			return RecheckBatchResult{}, err
		}
		invalidErrorTuple = true
		lastError = nil
	}
	if !invalidErrorTuple {
		if err := validateRecheckResultState(status, nextAttemptAt, firstError, lastError); err != nil {
			return RecheckBatchResult{}, err
		}
	}
	var queueDepthValue *int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&queueDepth); err != nil {
		runtimeLog.WarnContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to count durable recheck queue for result metadata", slog.String("stage", "queue_depth"), slog.String("event_family", "recheck_result"), slog.String("error", SanitizeRecheckError(err)))
	} else {
		value := int(queueDepth)
		queueDepthValue = &value
	}
	result = RecheckBatchResult{
		JobID: jobIDRead, TriggerType: triggerType, Status: status, QueueDepth: queueDepthValue,
		CreatedAt: createdAt, StartedAt: startedAt, FinishedAt: finishedAt,
		TotalCandidates: int(total), ProcessedCandidates: int(processed),
		OutcomeCounts: RecheckOutcomeCounts{Promoted: int(promoted), Ineligible: int(ineligible), MissingLabels: int(missing), IncompleteSources: int(incomplete), Stale: int(stale)},
		AttemptCount:  int(attempts), RetryCount: int(retries), ConsecutiveRetryCount: int(consecutive), NextAttemptAt: nextAttemptAt,
		FirstError: firstError, LastError: lastError, RecoveryCount: int(recovery),
	}
	if lastError != nil {
		result.FailureClass, result.FailureStage = lastError.Class, lastError.Stage
	}
	return result, nil
}

func (s *Store) readRecheckJobResult(ctx context.Context, jobID int64, strictErrorTuple bool) (RecheckBatchResult, error) {
	if s == nil || s.db == nil {
		return RecheckBatchResult{}, ErrStoreNotInitialized
	}
	if s.lifecycleTestHooks != nil {
		s.lifecycleTestHooks.mu.Lock()
		fault := s.lifecycleTestHooks.recheckResultReadFault
		s.lifecycleTestHooks.mu.Unlock()
		if fault != nil {
			if err := fault(); err != nil {
				return RecheckBatchResult{}, err
			}
		}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RecheckBatchResult{}, fmt.Errorf("failed to begin recheck result snapshot: %w", err)
	}
	result, readErr := readRecheckJobResultTx(ctx, tx, jobID, strictErrorTuple)
	if readErr != nil {
		_ = tx.Rollback()
		return RecheckBatchResult{}, readErr
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return RecheckBatchResult{}, fmt.Errorf("failed to commit recheck result snapshot: %w", commitErr)
	}
	return result, nil
}

func validateRecheckResultState(status string, nextAttemptAt *time.Time, firstError, lastError *RecheckErrorSnapshot) error {
	switch status {
	case "retry_wait":
		if nextAttemptAt == nil || firstError == nil || lastError == nil {
			return errors.New("retry_wait recheck result has incomplete durable metadata")
		}
	case "failed", "abandoned":
		if nextAttemptAt != nil || firstError == nil || lastError == nil {
			return fmt.Errorf("%s recheck result has incomplete durable metadata", status)
		}
	case "queued", "running", "completed":
		if nextAttemptAt != nil {
			return fmt.Errorf("%s recheck result has an invalid next_attempt_at", status)
		}
	default:
		return fmt.Errorf("unknown recheck result status %q", status)
	}
	return nil
}

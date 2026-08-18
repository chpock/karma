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

func (s *Store) RunSilenceRecheckBatchContext(ctx context.Context, batchSize int) (promotedResult int, resultErr error) {
	if s == nil || s.db == nil {
		return 0, ErrStoreNotInitialized
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	startedAt := time.Now().UTC()
	var selectedJobID int64
	var selectedJobStatus string
	var selectedQueueDepth, selectedRetryCount int
	var committedJobID int64
	var committedStatus string
	var committedStarted, committedCompleted bool
	var committedPriorProcessed, committedTotalCandidates, committedProcessed int
	var committedPromoted, committedStale, committedMissing, committedIneligible, committedIncomplete int
	var committedJobProcessed, committedJobPromoted, committedJobStale, committedJobMissing, committedJobIneligible, committedJobIncomplete int
	var committedPinnedRuleCount, committedSourceCount int
	var committedDecisions []runtimeEvent

	finalizer := &writerAfterCommitFinalizer{
		ctx:           ctx,
		operationKind: writerOperationRecheckBatch,
		run: func(reconciled bool) {
			if reconciled {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_commit_reconciled", "Silence recheck commit was reconciled after a delayed ledger lookup", slog.String("writer_operation", writerOperationRecheckBatch), slog.Int64("job_id", committedJobID), slog.Int("processed", committedProcessed), slog.Int("promoted", committedPromoted), slog.Duration("duration", time.Since(startedAt)))
			}
			if committedJobID == 0 {
				return
			}
			if committedStarted && committedStatus == "queued" {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_started", "Silence recheck job started", slog.Int64("job_id", committedJobID), slog.Int("candidate_count", committedTotalCandidates), slog.Int("pinned_rule_count", committedPinnedRuleCount), slog.Int("source_count", committedSourceCount))
			} else if committedStarted {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_resumed", "Silence recheck job resumed", slog.Int64("job_id", committedJobID), slog.Int("prior_processed", committedPriorProcessed), slog.Int("candidate_count", committedTotalCandidates), slog.Int("pinned_rule_count", committedPinnedRuleCount), slog.Int("source_count", committedSourceCount))
			}
			for _, event := range committedDecisions {
				emitRuntimeEventsContext(ctx, []runtimeEvent{event})
			}
			if committedCompleted {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_completed", "Silence recheck job completed", slog.Int64("job_id", committedJobID), slog.Int("processed", committedJobProcessed), slog.Int("promoted", committedJobPromoted), slog.Int("missing_labels", committedJobMissing), slog.Int("ineligible", committedJobIneligible), slog.Int("incomplete_sources", committedJobIncomplete), slog.Int("stale", committedJobStale), slog.Duration("duration", time.Since(startedAt)))
				return
			}
			level := slog.LevelDebug
			if committedPromoted > 0 {
				level = slog.LevelInfo
			}
			runtimeLog.Event(ctx, level, "silence_recheck", "batch_completed", "Silence recheck batch completed", slog.Int64("job_id", committedJobID), slog.Int("processed", committedProcessed), slog.Int("promoted", committedPromoted), slog.Int("stale", committedStale), slog.Int("missing_labels", committedMissing), slog.Int("ineligible", committedIneligible), slog.Int("incomplete_sources", committedIncomplete), slog.Duration("duration", time.Since(startedAt)))
		},
	}

	promotedResult, resultErr = withWriterTxValueAfterCommit(ctx, s, writerOperationRecheckBatch, func(tx *sql.Tx) (int, error) {
		jobID, _, status, err := selectNextRecheckJobTx(tx)
		if err != nil {
			return 0, err
		}
		if jobID == 0 {
			s.recheckBlockedJobLogged = 0
			runtimeLog.DebugContext(ctx, "silence_recheck", "queue_empty", "Silence recheck queue is empty")
			return 0, nil
		}
		queueDepth := -1
		if queryErr := tx.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&queueDepth); queryErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to count silence recheck queue for logging", slog.Int64("job_id", jobID), slog.Any("error", queryErr))
		}
		var priorProcessed, totalCandidates, retryCount int
		if queryErr := tx.QueryRow(`SELECT processed_candidates, total_candidates, retry_count FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&priorProcessed, &totalCandidates, &retryCount); queryErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to read silence recheck job progress for logging", slog.Int64("job_id", jobID), slog.Any("error", queryErr))
			priorProcessed, totalCandidates, retryCount = -1, -1, -1
		}
		selectedJobStatus, selectedQueueDepth, selectedRetryCount = status, queueDepth, retryCount
		runtimeLog.DebugContext(ctx, "silence_recheck", "job_selected", "Silence recheck job selected", slog.Int64("job_id", jobID), slog.String("status", status), slog.Int("queue_depth", queueDepth), slog.Int("retry_count", retryCount), slog.Int("batch_size", batchSize))
		if !RecheckJobCanRun(status, false) {
			var lastError sql.NullString
			_ = tx.QueryRow(`SELECT COALESCE(last_error_message, first_error_message, '') FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&lastError)
			if s.recheckBlockedJobLogged != jobID {
				runtimeLog.WarnContext(ctx, "silence_recheck", "failed_job_blocks_queue", "Failed silence recheck job blocks the queue", slog.Int64("job_id", jobID), slog.Int("queue_depth", queueDepth))
				s.recheckBlockedJobLogged = jobID
			} else {
				runtimeLog.DebugContext(ctx, "silence_recheck", "failed_job_blocks_queue", "Failed silence recheck job still blocks the queue", slog.Int64("job_id", jobID), slog.Int("queue_depth", queueDepth))
			}
			return 0, fmt.Errorf("silence recheck job %d is %s and blocks later jobs: %s", jobID, status, lastError.String)
		}
		s.recheckBlockedJobLogged = 0
		selectedJobID = jobID
		now := time.Now().UTC()
		nowRaw := now.Format(time.RFC3339Nano)
		if status == "queued" {
			if _, err = tx.Exec(`UPDATE silence_recheck_jobs SET status='running', started_at=COALESCE(started_at, ?) WHERE job_id=?`, nowRaw, jobID); err != nil {
				return 0, fmt.Errorf("failed to mark recheck job running: %w", err)
			}
		}
		rules, err := loadRecheckJobRulesTx(tx, jobID)
		if err != nil {
			return 0, err
		}
		committedPinnedRuleCount = len(rules)
		sourceStatuses, err := loadRecheckJobSourceStatusesTx(tx, jobID)
		if err != nil {
			return 0, err
		}
		committedSourceCount = len(sourceStatuses)
		candidates, err := loadRecheckCandidatesTx(tx, jobID, batchSize)
		if err != nil {
			return 0, err
		}
		if _, _, accountingErr := validateRecheckJobAccountingTx(tx, jobID); accountingErr != nil {
			return 0, accountingErr
		}
		if len(candidates) == 0 {
			accounting, complete, accountingErr := validateRecheckJobAccountingTx(tx, jobID)
			if accountingErr != nil {
				return 0, accountingErr
			}
			if !complete {
				return 0, fmt.Errorf("recheck job %d is not complete despite having no pending candidates", jobID)
			}
			if _, err = tx.Exec(`UPDATE silence_recheck_jobs SET status='completed', finished_at=?, last_error_message=NULL, last_error_class=NULL, last_error_stage=NULL, last_error_at=NULL WHERE job_id=?`, nowRaw, jobID); err != nil {
				return 0, fmt.Errorf("failed to mark recheck job completed: %w", err)
			}
			committedJobID, committedStatus, committedStarted, committedCompleted = jobID, status, true, true
			committedJobProcessed, committedJobPromoted, committedJobMissing, committedJobIneligible, committedJobIncomplete, committedJobStale, committedTotalCandidates = accounting.Processed, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale, accounting.Total
			return 0, nil
		}

		processed, promoted, stale, missing, ineligible, incomplete := 0, 0, 0, 0, 0, 0
		decisions := make([]runtimeEvent, 0, len(candidates))
		for _, candidate := range candidates {
			processed++
			decision, decisionErr := processRecheckCandidateTx(tx, candidate, rules, sourceStatuses, jobID, now, s.recheckPostCASReadError)
			if decisionErr != nil {
				return 0, decisionErr
			}
			if decision.outcome == "" {
				return 0, errors.New("recheck candidate returned no terminal outcome")
			}
			if err = updateRecheckCandidateTerminalAndCounterTx(tx, jobID, candidate, now, decision.outcome); err != nil {
				return 0, fmt.Errorf("failed marking recheck candidate terminal: %w", err)
			}
			if decision.promoted {
				promoted++
			} else {
				decisions = append(decisions, recheckCandidateSkippedEvent(jobID, candidate.AlertID, decision.outcome))
			}
			decisions = append(decisions, decision.events...)
			switch decision.outcome {
			case "promoted":
			case "stale_alert_observation", "stale_target_observation":
				stale++
			case "missing_labels", "invalid_labels":
				missing++
			case "incomplete_source_coverage":
				incomplete++
			default:
				ineligible++
			}
		}
		accounting, complete, accountingErr := validateRecheckJobAccountingTx(tx, jobID)
		if accountingErr != nil {
			return 0, accountingErr
		}
		committedJobProcessed, committedJobPromoted, committedJobMissing, committedJobIneligible, committedJobIncomplete, committedJobStale = accounting.Processed, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale
		if complete {
			if _, err = tx.Exec(`UPDATE silence_recheck_jobs SET status='completed', finished_at=?, last_error_message=NULL, last_error_class=NULL, last_error_stage=NULL, last_error_at=NULL WHERE job_id=?`, nowRaw, jobID); err != nil {
				return 0, fmt.Errorf("failed to complete recheck job: %w", err)
			}
			committedCompleted = true
		}
		committedDecisions = decisions
		committedJobID, committedStatus, committedStarted = jobID, status, true
		committedPriorProcessed, committedTotalCandidates = priorProcessed, totalCandidates
		committedPromoted, committedProcessed, committedStale, committedMissing, committedIneligible, committedIncomplete = promoted, processed, stale, missing, ineligible, incomplete
		return promoted, nil
	}, finalizer)
	if resultErr != nil && selectedJobID != 0 {
		if errors.Is(resultErr, ErrWriterCommitOutcomeUnknown) {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "job_commit_outcome_unknown", "Silence recheck job commit outcome is unknown; job failure was not persisted", slog.String("writer_operation", writerOperationRecheckBatch), slog.Int64("job_id", selectedJobID), slog.String("status", selectedJobStatus), slog.Int("queue_depth", selectedQueueDepth), slog.Int("retry_count", selectedRetryCount), slog.String("stage", "commit_reconcile"), slog.Duration("duration", time.Since(startedAt)), slog.Any("error", resultErr))
			return promotedResult, resultErr
		}
		if reason := recheckSelectedInfrastructureReason(ctx, resultErr); reason != "" {
			if contractErr := emitRecheckInfrastructureBlocked(ctx, selectedJobID, writerOperationRecheckBatch, reason, resultErr); contractErr != nil {
				return promotedResult, errors.Join(resultErr, contractErr)
			}
			return promotedResult, resultErr
		}
		return promotedResult, s.persistRecheckFailure(ctx, selectedJobID, resultErr, startedAt)
	}
	return promotedResult, resultErr
}

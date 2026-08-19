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
	if pending := s.pendingRecheckFailureSnapshot(); pending != nil {
		reconciliation, reconcileErr := s.reconcileWriterForRecheck(ctx)
		if reconcileErr != nil {
			return 0, reconcileErr
		}
		failureResult, persistErr := s.persistRecheckAttemptFailure(ctx, pending.Attempt)
		if persistErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "failure_persistence_failed", "Silence recheck failure bookkeeping was not confirmed", slog.Int64("job_id", pending.Attempt.JobID), slog.String("stage", recheckStagePersistFailure), slog.Any("error", persistErr))
			return 0, &recheckFailurePersistenceError{err: persistErr, reconciliationResolved: reconciliation.resolved, retryable: IsRetryableRecheckFailurePersistence(persistErr)}
		}
		s.clearPendingRecheckFailure(pending.Attempt.Token)
		if failureResult.RetryScheduled && failureResult.NextAttemptAt != nil {
			runtimeLog.InfoContext(ctx, "silence_recheck", "retry_scheduled", "Silence recheck retry was durably scheduled", slog.Int64("job_id", pending.Attempt.JobID), slog.String("next_attempt_at", failureResult.NextAttemptAt.UTC().Format(time.RFC3339Nano)), slog.String("failure_class", string(pending.Attempt.FailureClass)), slog.String("failure_stage", pending.Attempt.FailureStage))
			return 0, &recheckRetryScheduledError{JobID: pending.Attempt.JobID, NextAttemptAt: *failureResult.NextAttemptAt, Cause: pending.Attempt.FailureCause}
		}
		if failureResult.Status == "committed" || failureResult.Status == "failed" {
			if failureResult.Status == "failed" {
				runtimeLog.ErrorContext(ctx, "silence_recheck", "job_failed", "Silence recheck job reached terminal failure", slog.Int64("job_id", pending.Attempt.JobID), slog.String("failure_class", string(pending.Attempt.FailureClass)), slog.String("failure_stage", pending.Attempt.FailureStage))
				return 0, NewRecheckDurableFailureError(pending.Attempt.JobID, pending.Attempt.FailureCause)
			}
			return 0, nil
		}
		return 0, pending.Attempt.FailureCause
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
	var selectedAttempt *recheckAttemptInfo

	finalizer := &writerAfterCommitFinalizer{
		ctx:           ctx,
		operationKind: writerOperationRecheckBatch,
		handoff: func(disposition writerHandoffDisposition) {
			if disposition == writerHandoffCrossOperation && selectedAttempt != nil {
				s.handoffRecheckReconciledLocked()
			}
		},
		reconcile: func(writerCommitted bool) error {
			if selectedAttempt == nil {
				return nil
			}
			if !writerCommitted {
				if err := s.validateRecheckAttemptAbsenceLocked(ctx, *selectedAttempt); err != nil {
					return err
				}
				selectedAttempt.AllowCommitted = false
				s.disallowCommittedPendingRecheckAttempt(selectedAttempt.Token)
				return nil
			}
			return s.validateRecheckAttemptTupleLocked(ctx, *selectedAttempt, "committed", false)
		},
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
		if faultErr := s.recheckStageFault(recheckStageSelectJob); faultErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("select_job", faultErr))
		}
		jobID, _, status, nextAttemptRaw, err := selectNextRecheckJobWithDueTx(tx)
		if err != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("select_job", err))
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
		var priorProcessed, totalCandidates, retryCount, consecutiveRetryCount int
		if queryErr := tx.QueryRow(`SELECT processed_candidates, total_candidates, retry_count, consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&priorProcessed, &totalCandidates, &retryCount, &consecutiveRetryCount); queryErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("read_job_state", queryErr))
		}
		selectedJobStatus, selectedQueueDepth, selectedRetryCount = status, queueDepth, retryCount
		runtimeLog.DebugContext(ctx, "silence_recheck", "job_selected", "Silence recheck job selected", slog.Int64("job_id", jobID), slog.String("status", status), slog.Int("queue_depth", queueDepth), slog.Int("retry_count", retryCount), slog.Int("batch_size", batchSize))
		s.recheckBlockedJobLogged = 0
		now := time.Now().UTC()
		var nextAttemptAt *time.Time
		if nextAttemptRaw != nil {
			parsed, parseErr := time.Parse(time.RFC3339Nano, *nextAttemptRaw)
			if parseErr != nil {
				return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("invalid_next_attempt_at", parseErr))
			}
			nextAttemptAt = &parsed
		}
		if status == "failed" || (status == "retry_wait" && !recheckQueueEligible(status, nextAttemptAt, now)) {
			if s.recheckBlockedJobLogged != jobID {
				runtimeLog.WarnContext(ctx, "silence_recheck", "failed_job_blocks_queue", "Recheck queue head blocks later jobs", slog.Int64("job_id", jobID), slog.String("status", status), slog.Int("queue_depth", queueDepth))
				s.recheckBlockedJobLogged = jobID
			}
			return 0, &recheckQueueBlockedError{JobID: jobID, Status: status, NextAttemptAt: nextAttemptAt}
		}
		if !RecheckJobCanRun(status, nextAttemptAt, now) {
			return 0, &recheckQueueBlockedError{JobID: jobID, Status: status, NextAttemptAt: nextAttemptAt}
		}
		selectedJobID = jobID
		selectedTransition, transitionErr := applyRecheckJobEvent(recheckJobState{Status: status, RetryCount: retryCount, ConsecutiveRetryCount: consecutiveRetryCount, NextAttemptAt: nextAttemptAt}, recheckJobSelected, now)
		if transitionErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, fmt.Errorf("%w: selection transition: %w", ErrWriterOperationInvariant, transitionErr))
		}
		attempt, attemptErr := s.createRecheckAttemptTx(tx, jobID, now)
		if attemptErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("create_attempt", attemptErr))
		}
		attempt.RetryCountBefore = retryCount
		attempt.ConsecutiveRetryCountBefore = consecutiveRetryCount
		selectedAttempt = &attempt
		nowRaw := now.Format(time.RFC3339Nano)
		if _, err = tx.Exec(`UPDATE silence_recheck_jobs SET status=?, next_attempt_at=?, finished_at=?, started_at=COALESCE(started_at, ?) WHERE job_id=?`, selectedTransition.Status, nullableTime(selectedTransition.NextAttemptAt), nullableTime(selectedTransition.FinishedAt), nowRaw, jobID); err != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, fmt.Errorf("failed to persist selected recheck transition: %w", err))
		}
		rules, err := loadRecheckJobRulesTx(tx, jobID)
		if err != nil {
			return 0, wrapRecheckStage(recheckStageLoadContext, err)
		}
		committedPinnedRuleCount = len(rules)
		sourceStatuses, err := loadRecheckJobSourceStatusesTx(tx, jobID)
		if err != nil {
			return 0, wrapRecheckStage(recheckStageLoadContext, err)
		}
		committedSourceCount = len(sourceStatuses)
		candidates, err := loadRecheckCandidatesTx(tx, jobID, batchSize)
		if err != nil {
			return 0, wrapRecheckStage(recheckStageLoadCandidates, err)
		}
		if _, _, accountingErr := validateRecheckJobAccountingTx(tx, jobID); accountingErr != nil {
			return 0, wrapRecheckStage(recheckStageUpdateProgress, accountingErr)
		}
		if len(candidates) == 0 {
			accounting, complete, accountingErr := validateRecheckJobAccountingTx(tx, jobID)
			if accountingErr != nil {
				return 0, wrapRecheckStage(recheckStageUpdateProgress, accountingErr)
			}
			if !complete {
				return 0, wrapRecheckStage(recheckStageUpdateProgress, fmt.Errorf("recheck job %d is not complete despite having no pending candidates", jobID))
			}
			transition, completionTransitionErr := applyRecheckJobEvent(recheckJobState{Status: "running", RetryCount: retryCount, ConsecutiveRetryCount: consecutiveRetryCount}, recheckJobBatchCompleted, now)
			if completionTransitionErr != nil {
				return 0, fmt.Errorf("%w: completion transition: %w", ErrWriterOperationInvariant, completionTransitionErr)
			}
			if err = s.commitRecheckAttemptTx(tx, *selectedAttempt, jobID, transition, now); err != nil {
				return 0, wrapRecheckStage(recheckStageUpdateProgress, err)
			}
			committedJobID, committedStatus, committedStarted, committedCompleted = jobID, status, true, true
			committedJobProcessed, committedJobPromoted, committedJobMissing, committedJobIneligible, committedJobIncomplete, committedJobStale, committedTotalCandidates = accounting.Processed, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale, accounting.Total
			return 0, nil
		}

		processed, promoted, stale, missing, ineligible, incomplete := 0, 0, 0, 0, 0, 0
		decisions := make([]runtimeEvent, 0, len(candidates))
		for _, candidate := range candidates {
			if faultErr := s.recheckStageFault(recheckStagePersistCandidate); faultErr != nil {
				return 0, wrapRecheckStage(recheckStagePersistCandidate, faultErr)
			}
			processed++
			decision, decisionErr := processRecheckCandidateTx(tx, candidate, rules, sourceStatuses, jobID, now, s.recheckPostCASReadError)
			if decisionErr != nil {
				return 0, wrapRecheckStage(recheckStageEvaluate, decisionErr)
			}
			if decision.outcome == "" {
				return 0, wrapRecheckStage(recheckStageEvaluate, errors.New("recheck candidate returned no terminal outcome"))
			}
			if err = updateRecheckCandidateTerminalAndCounterTx(tx, jobID, candidate, now, decision.outcome); err != nil {
				return 0, wrapRecheckStage(recheckStagePersistCandidate, fmt.Errorf("failed marking recheck candidate terminal: %w", err))
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
			return 0, wrapRecheckStage(recheckStageUpdateProgress, accountingErr)
		}
		committedJobProcessed, committedJobPromoted, committedJobMissing, committedJobIneligible, committedJobIncomplete, committedJobStale = accounting.Processed, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale
		transitionEvent := recheckJobBatchCommitted
		if complete {
			transitionEvent = recheckJobBatchCompleted
			committedCompleted = true
		}
		transition, transitionErr := applyRecheckJobEvent(recheckJobState{Status: "running", RetryCount: retryCount, ConsecutiveRetryCount: consecutiveRetryCount}, transitionEvent, now)
		if transitionErr != nil {
			return 0, fmt.Errorf("%w: batch transition: %w", ErrWriterOperationInvariant, transitionErr)
		}
		if err = s.commitRecheckAttemptTx(tx, *selectedAttempt, jobID, transition, now); err != nil {
			return 0, wrapRecheckStage(recheckStageUpdateProgress, err)
		}
		committedDecisions = decisions
		committedJobID, committedStatus, committedStarted = jobID, status, true
		committedPriorProcessed, committedTotalCandidates = priorProcessed, totalCandidates
		committedPromoted, committedProcessed, committedStale, committedMissing, committedIneligible, committedIncomplete = promoted, processed, stale, missing, ineligible, incomplete
		return promoted, nil
	}, finalizer)
	if resultErr != nil && selectedJobID != 0 {
		if ctx.Err() != nil || context.Cause(ctx) != nil {
			return promotedResult, resultErr
		}
		var commitOutcomeErr *writerCommitOutcomeError
		hasCommitOutcome := errors.As(resultErr, &commitOutcomeErr)
		if hasCommitOutcome && commitOutcomeErr.outcome == writerCommitOutcomeUnknown {
			if selectedAttempt != nil {
				selectedAttempt.FailureCause = commitOutcomeErr.OriginalCause()
				selectedAttempt.FailureStage = recheckStageCommit
				selectedAttempt.FailureClass = classifyRecheckFailure(selectedAttempt.FailureCause)
				selectedAttempt.AllowCommitted = true
				s.setPendingRecheckFailure(*selectedAttempt)
			}
			runtimeLog.ErrorContext(ctx, "silence_recheck", "job_commit_outcome_unknown", "Silence recheck job commit outcome is unknown; job failure was not persisted", slog.String("writer_operation", writerOperationRecheckBatch), slog.Int64("job_id", selectedJobID), slog.String("status", selectedJobStatus), slog.Int("queue_depth", selectedQueueDepth), slog.Int("retry_count", selectedRetryCount), slog.String("stage", "commit_reconcile"), slog.Duration("duration", time.Since(startedAt)), slog.Any("error", resultErr))
			return promotedResult, resultErr
		}
		commitNotCommitted := hasCommitOutcome && commitOutcomeErr.outcome == writerCommitOutcomeNotCommitted && selectedAttempt != nil
		if commitNotCommitted {
			selectedAttempt.FailureCause = commitOutcomeErr.OriginalCause()
			selectedAttempt.FailureStage = recheckStageCommit
			selectedAttempt.FailureClass = classifyRecheckFailure(selectedAttempt.FailureCause)
			selectedAttempt.AllowCommitted = false
		} else if reason := recheckSelectedInfrastructureReason(ctx, resultErr); reason != "" {
			if contractErr := emitRecheckInfrastructureBlocked(ctx, selectedJobID, writerOperationRecheckBatch, reason, resultErr); contractErr != nil {
				return promotedResult, errors.Join(resultErr, contractErr)
			}
			return promotedResult, resultErr
		}
		if selectedAttempt == nil {
			return promotedResult, resultErr
		}
		if !commitNotCommitted {
			selectedAttempt.FailureCause = resultErr
			selectedAttempt.FailureStage = recheckFailureStage(resultErr)
			selectedAttempt.FailureClass = classifyRecheckFailure(resultErr)
		}
		failureResult, persistErr := s.persistRecheckAttemptFailure(ctx, *selectedAttempt)
		if persistErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "failure_persistence_failed", "Silence recheck failure bookkeeping was not confirmed", slog.Int64("job_id", selectedJobID), slog.String("stage", recheckStagePersistFailure), slog.Any("error", persistErr))
			s.setPendingRecheckFailure(*selectedAttempt)
			return promotedResult, persistErr
		}
		if failureResult.RetryScheduled && failureResult.NextAttemptAt != nil {
			runtimeLog.InfoContext(ctx, "silence_recheck", "retry_scheduled", "Silence recheck retry was durably scheduled", slog.Int64("job_id", selectedJobID), slog.String("next_attempt_at", failureResult.NextAttemptAt.UTC().Format(time.RFC3339Nano)), slog.String("failure_class", string(selectedAttempt.FailureClass)), slog.String("failure_stage", selectedAttempt.FailureStage))
			return promotedResult, &recheckRetryScheduledError{JobID: selectedJobID, NextAttemptAt: *failureResult.NextAttemptAt, Cause: resultErr}
		}
		runtimeLog.ErrorContext(ctx, "silence_recheck", "job_failed", "Silence recheck job reached terminal failure", slog.Int64("job_id", selectedJobID), slog.String("failure_class", string(selectedAttempt.FailureClass)), slog.String("failure_stage", selectedAttempt.FailureStage))
		return promotedResult, NewRecheckDurableFailureError(selectedJobID, resultErr)
	}
	return promotedResult, resultErr
}

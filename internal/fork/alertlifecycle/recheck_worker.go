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

func (s *Store) RunSilenceRecheckBatchContext(ctx context.Context, batchSize int) (RecheckBatchResult, error) {
	for {
		if completion := s.pendingRecheckOperatorCompletion(); completion != nil {
			if err := completion.waitFinalized(ctx); err != nil {
				return RecheckBatchResult{}, err
			}
		}
		s.signalRecheckWorkerBarrier("after_precheck")
		result, err := s.runSilenceRecheckBatchContext(ctx, batchSize)
		if !errors.Is(err, errRecheckOperatorFinalizing) {
			return result, err
		}
		if completion := s.pendingRecheckOperatorCompletion(); completion != nil {
			if waitErr := completion.waitFinalized(ctx); waitErr != nil {
				return RecheckBatchResult{}, waitErr
			}
		}
	}
}

func (s *Store) runSilenceRecheckBatchContext(ctx context.Context, batchSize int) (batchResult RecheckBatchResult, resultErr error) {
	if s == nil || s.db == nil {
		return RecheckBatchResult{}, ErrStoreNotInitialized
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if pending := s.pendingRecheckFailureSnapshot(); pending != nil {
		reconciliation, reconcileErr := s.reconcileWriterForRecheck(ctx, *pending)
		if reconciliation.outcome == "committed" && reconciliation.result != nil {
			result := *reconciliation.result
			result.ReconciledOperation = reconciliation.operation
			switch reconciliation.operation {
			case writerOperationRecheckBatch:
				result.Disposition = RecheckBatchCommitted
				return result, nil
			case writerOperationRecheckFailure:
				result.FailureClass = string(pending.Attempt.FailureClass)
				result.FailureStage = pending.Attempt.FailureStage
				if result.Status == "retry_wait" && result.NextAttemptAt != nil {
					result.Disposition = RecheckBatchRetryScheduled
					return result, &recheckRetryScheduledError{JobID: pending.Attempt.JobID, NextAttemptAt: *result.NextAttemptAt, Cause: pending.Attempt.FailureCause}
				}
				if result.Status == "failed" {
					result.Disposition = RecheckBatchJobFailed
					return result, NewRecheckDurableFailureError(pending.Attempt.JobID, pending.Attempt.FailureCause)
				}
			}
		}
		if reconciliation.outcome == "committed" && reconciliation.operation != "" && !isRecheckWriterOperation(reconciliation.operation) {
			reconcileErr = nil
		}
		if reconcileErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "failure_persistence_failed", "Silence recheck failure bookkeeping was not confirmed", slog.Int64("job_id", pending.Attempt.JobID), slog.String("stage", recheckStagePersistFailure), slog.String("error", SanitizeRecheckError(reconcileErr)))
			return RecheckBatchResult{Disposition: RecheckBatchFailurePersistenceFailed, JobID: pending.Attempt.JobID}, reconcileErr
		}
		if pending.Operation != writerOperationRecheckFailure {
			failurePending := *pending
			failurePending.Operation = writerOperationRecheckFailure
			failurePending.Result = RecheckBatchResult{Disposition: RecheckBatchFailurePersistenceFailed, JobID: pending.Attempt.JobID, FailureClass: string(pending.Attempt.FailureClass), FailureStage: pending.Attempt.FailureStage}
			failurePending.ResultReady = false
			failurePending.Completion = newRecheckFailureCompletion()
			s.setPendingRecheckFailure(failurePending)
			pending = &failurePending
		}
		failureResult, persistErr := s.persistRecheckAttemptFailure(ctx, pending.Attempt)
		if persistErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "failure_persistence_failed", "Silence recheck failure bookkeeping was not confirmed", slog.Int64("job_id", pending.Attempt.JobID), slog.String("stage", recheckStagePersistFailure), slog.String("error", SanitizeRecheckError(persistErr)))
			return RecheckBatchResult{Disposition: RecheckBatchFailurePersistenceFailed, JobID: pending.Attempt.JobID, FailureClass: string(pending.Attempt.FailureClass), FailureStage: recheckStagePersistFailure}, &recheckFailurePersistenceError{err: persistErr, reconciliationResolved: reconciliation.resolved, retryable: IsRetryableRecheckFailurePersistence(persistErr)}
		}
		if failureResult.RetryScheduled && failureResult.NextAttemptAt != nil {
			result := RecheckBatchResult{Disposition: RecheckBatchRetryScheduled, JobID: pending.Attempt.JobID, Status: "retry_wait", NextAttemptAt: failureResult.NextAttemptAt, FailureClass: string(pending.Attempt.FailureClass), FailureStage: pending.Attempt.FailureStage}
			if failureResult.Snapshot != nil {
				result = *failureResult.Snapshot
				result.Disposition = RecheckBatchRetryScheduled
				result.FailureClass, result.FailureStage = string(pending.Attempt.FailureClass), pending.Attempt.FailureStage
			}
			return result, &recheckRetryScheduledError{JobID: pending.Attempt.JobID, NextAttemptAt: *failureResult.NextAttemptAt, Cause: pending.Attempt.FailureCause}
		}
		if failureResult.Status == "committed" || failureResult.Status == "failed" {
			if failureResult.Status == "failed" {
				result := RecheckBatchResult{Disposition: RecheckBatchJobFailed, JobID: pending.Attempt.JobID, Status: "failed", FailureClass: string(pending.Attempt.FailureClass), FailureStage: pending.Attempt.FailureStage}
				if failureResult.Snapshot != nil {
					result = *failureResult.Snapshot
					result.Disposition = RecheckBatchJobFailed
					result.FailureClass, result.FailureStage = string(pending.Attempt.FailureClass), pending.Attempt.FailureStage
				}
				return result, NewRecheckDurableFailureError(pending.Attempt.JobID, pending.Attempt.FailureCause)
			}
			result := RecheckBatchResult{Disposition: RecheckBatchCommitted, JobID: pending.Attempt.JobID}
			if failureResult.Snapshot != nil {
				result = *failureResult.Snapshot
				result.Disposition = RecheckBatchCommitted
			}
			return result, nil
		}
		return RecheckBatchResult{Disposition: RecheckBatchFailurePersistenceFailed, JobID: pending.Attempt.JobID, FailureClass: string(pending.Attempt.FailureClass), FailureStage: pending.Attempt.FailureStage}, pending.Attempt.FailureCause
	}
	startedAt := s.currentRecheckTime()
	var selectedJobID int64
	var selectedJobStatus, selectedTriggerType string
	var selectedCreatedAt *time.Time
	var selectedQueueDepth *int
	queueDepthMetadataUnavailable := false
	var selectedRetryCount int
	var selectedPriorProcessed int
	var committedJobID int64
	var committedStatus string
	var committedTotalCandidates, committedProcessed int
	var committedPromoted, committedStale, committedMissing, committedIneligible, committedIncomplete int
	var batchProcessed, batchPromoted, batchStale, batchMissing, batchIneligible, batchIncomplete int
	var committedPinnedRuleCount, committedSourceCount int
	var committedDecisions []runtimeEvent
	var committedResult *RecheckBatchResult
	var batchDuration time.Duration
	var selectedAttempt *recheckAttemptInfo
	var blockedJobID int64
	var blockedQueueDepth *int
	selectedResult := func(disposition RecheckBatchDisposition, status string) RecheckBatchResult {
		result := RecheckBatchResult{Disposition: disposition, JobID: selectedJobID, TriggerType: selectedTriggerType, Status: status, QueueDepth: selectedQueueDepth, CreatedAt: selectedCreatedAt}
		return result
	}
	batchCompletion := newRecheckFailureCompletion()

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
			if err := s.validateRecheckAttemptTupleLocked(ctx, *selectedAttempt, "committed", false); err != nil {
				return err
			}
			batchCompletion.completeCommitted()
			return nil
		},
		run: func(reconciled bool) {
			if reconciled && selectedAttempt != nil {
				defer batchCompletion.completeFinalized()
				defer s.clearPendingRecheckFailure(selectedAttempt.Token)
			}
			if reconciled {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_commit_reconciled", "Silence recheck commit was reconciled after a delayed ledger lookup", slog.String("writer_operation", writerOperationRecheckBatch), slog.Int64("job_id", committedJobID), slog.Int("processed", committedProcessed), slog.Int("promoted", committedPromoted), slog.Duration("duration", batchDuration))
			}
			if committedJobID == 0 {
				return
			}
			selectedStatus, selectedQueue, selectedAttempts, selectedRetries := selectedJobStatus, selectedQueueDepth, 0, selectedRetryCount
			if committedResult != nil {
				selectedAttempts, selectedRetries = committedResult.AttemptCount, committedResult.RetryCount
			}
			logRecheckJobSelected(ctx, committedJobID, selectedStatus, selectedQueue, selectedAttempts, selectedRetries, batchSize)
			if selectedJobStatus == "queued" {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_started", "Silence recheck job started", slog.Int64("job_id", committedJobID), slog.Int("candidate_count", committedTotalCandidates), slog.Int("pinned_rule_count", committedPinnedRuleCount), slog.Int("source_count", committedSourceCount))
			} else {
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_resumed", "Silence recheck job resumed", slog.Int64("job_id", committedJobID), slog.Int("prior_processed", selectedPriorProcessed), slog.Int("candidate_count", committedTotalCandidates), slog.Int("pinned_rule_count", committedPinnedRuleCount), slog.Int("source_count", committedSourceCount))
			}
			for _, event := range committedDecisions {
				s.emitRuntimeEventsBestEffort(ctx, "recheck_batch", []runtimeEvent{event})
			}
			if committedStatus == "completed" {
				s.logRecheckBatchCompleted(ctx, committedJobID, batchProcessed, batchPromoted, batchMissing, batchIneligible, batchIncomplete, batchStale, batchDuration, batchPromoted > 0)
				runtimeLog.InfoContext(ctx, "silence_recheck", "job_completed", "Silence recheck job completed", slog.Int64("job_id", committedJobID), slog.Int("processed", committedProcessed), slog.Int("promoted", committedPromoted), slog.Int("missing_labels", committedMissing), slog.Int("ineligible", committedIneligible), slog.Int("incomplete_sources", committedIncomplete), slog.Int("stale", committedStale), slog.Duration("duration", recheckJobDuration(committedResult, startedAt)))
				return
			}
			s.logRecheckBatchCompleted(ctx, committedJobID, batchProcessed, batchPromoted, batchMissing, batchIneligible, batchIncomplete, batchStale, batchDuration, batchPromoted > 0)
		},
	}

	_, resultErr = withWriterTxValueAfterCommit(ctx, s, writerOperationRecheckBatch, func(tx *sql.Tx) (int, error) {
		if s.pendingRecheckOperatorCompletion() != nil {
			s.signalRecheckWorkerBarrier("inside_transaction")
			return 0, errRecheckOperatorFinalizing
		}
		defer func() {
			batchDuration = time.Since(startedAt)
			if s.lifecycleTestHooks != nil {
				s.lifecycleTestHooks.mu.Lock()
				override := s.lifecycleTestHooks.recheckBatchDuration
				s.lifecycleTestHooks.mu.Unlock()
				if override != nil {
					batchDuration = override(batchDuration)
				}
			}
		}()
		if faultErr := s.recheckStageFault(recheckStageSelectJob); faultErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("select_job", faultErr))
		}
		jobID, triggerTypeRaw, status, nextAttemptRaw, err := selectNextRecheckJobWithDueTx(tx)
		if err != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("select_job", err))
		}
		if jobID == 0 {
			var queueDepth int
			if queueErr := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&queueDepth); queueErr != nil {
				runtimeLog.WarnContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to count empty recheck queue for result metadata", slog.String("stage", "queue_depth"), slog.String("event_family", "recheck_result"), slog.String("error", SanitizeRecheckError(queueErr)))
				queueDepthMetadataUnavailable = true
				selectedQueueDepth = nil
			} else {
				selectedQueueDepth = &queueDepth
			}
			return 0, nil
		}
		var queueDepthValue int
		var queueDepth *int
		queueDepthErr := s.observabilityCollectionFault("queue_depth", "recheck_batch")
		if queueDepthErr == nil {
			queueDepthErr = tx.QueryRow(`SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed')`).Scan(&queueDepthValue)
			if queueDepthErr == nil {
				queueDepth = &queueDepthValue
			}
		}
		if queueDepthErr != nil {
			queueDepthMetadataUnavailable = true
			runtimeLog.WarnContext(ctx, "silence_recheck", "observability_collection_failed", "Failed to count silence recheck queue for logging", slog.Int64("job_id", jobID), slog.String("stage", "queue_depth"), slog.String("event_family", "recheck_batch"), slog.String("error", SanitizeRecheckError(queueDepthErr)))
		}
		var priorProcessed, totalCandidates, retryCount, consecutiveRetryCount int
		if queryErr := tx.QueryRow(`SELECT processed_candidates, total_candidates, retry_count, consecutive_retry_count FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&priorProcessed, &totalCandidates, &retryCount, &consecutiveRetryCount); queryErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("read_job_state", queryErr))
		}
		triggerType := ""
		if triggerTypeRaw != nil {
			triggerType = *triggerTypeRaw
		}
		selectedJobStatus, selectedTriggerType, selectedQueueDepth, selectedRetryCount, selectedPriorProcessed = status, triggerType, queueDepth, retryCount, priorProcessed
		createdAtRaw := ""
		if queryErr := tx.QueryRow(`SELECT created_at FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&createdAtRaw); queryErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("read_job_created_at", queryErr))
		}
		createdAt, parseErr := time.Parse(time.RFC3339Nano, createdAtRaw)
		if parseErr != nil {
			return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("invalid_created_at", parseErr))
		}
		createdAt = createdAt.UTC()
		selectedCreatedAt = &createdAt
		now := s.currentRecheckTime()
		var nextAttemptAt *time.Time
		if nextAttemptRaw != nil {
			parsed, parseErr := time.Parse(time.RFC3339Nano, *nextAttemptRaw)
			if parseErr != nil {
				return 0, wrapRecheckStage(recheckStageSelectJob, newRecheckPreAttemptError("invalid_next_attempt_at", parseErr))
			}
			nextAttemptAt = &parsed
		}
		if status == "failed" || (status == "retry_wait" && !recheckQueueEligible(status, nextAttemptAt, now)) {
			blockedJobID, blockedQueueDepth = jobID, queueDepth
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
			committedJobID, committedStatus = jobID, transition.Status
			committedProcessed, committedPromoted, committedMissing, committedIneligible, committedIncomplete, committedStale, committedTotalCandidates = accounting.Processed, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale, accounting.Total
			snapshot, snapshotErr := readRecheckJobResultTx(ctx, tx, jobID, false)
			if snapshotErr != nil {
				return 0, wrapRecheckStage(recheckStageUpdateProgress, snapshotErr)
			}
			if queueDepthMetadataUnavailable {
				snapshot.QueueDepth = nil
			}
			committedResult = &snapshot
			s.setPendingRecheckFailure(recheckPendingFailure{Attempt: *selectedAttempt, Operation: writerOperationRecheckBatch, Result: snapshot, ResultReady: true, Selection: recheckFailureSelection{Status: selectedJobStatus, QueueDepth: selectedQueueDepth, BatchSize: batchSize}, Completion: batchCompletion})
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
		committedProcessed, committedPromoted, committedMissing, committedIneligible, committedIncomplete, committedStale, committedTotalCandidates = accounting.Processed, accounting.Promoted, accounting.MissingLabels, accounting.Ineligible, accounting.Incomplete, accounting.Stale, accounting.Total
		transitionEvent := recheckJobBatchCommitted
		if complete {
			transitionEvent = recheckJobBatchCompleted
		}
		transition, transitionErr := applyRecheckJobEvent(recheckJobState{Status: "running", RetryCount: retryCount, ConsecutiveRetryCount: consecutiveRetryCount}, transitionEvent, now)
		if transitionErr != nil {
			return 0, fmt.Errorf("%w: batch transition: %w", ErrWriterOperationInvariant, transitionErr)
		}
		if err = s.commitRecheckAttemptTx(tx, *selectedAttempt, jobID, transition, now); err != nil {
			return 0, wrapRecheckStage(recheckStageUpdateProgress, err)
		}
		committedDecisions = decisions
		committedJobID, committedStatus = jobID, transition.Status
		batchProcessed, batchPromoted, batchStale, batchMissing, batchIneligible, batchIncomplete = processed, promoted, stale, missing, ineligible, incomplete
		snapshot, snapshotErr := readRecheckJobResultTx(ctx, tx, jobID, false)
		if snapshotErr != nil {
			return 0, wrapRecheckStage(recheckStageUpdateProgress, snapshotErr)
		}
		if queueDepthMetadataUnavailable {
			snapshot.QueueDepth = nil
		}
		committedResult = &snapshot
		s.setPendingRecheckFailure(recheckPendingFailure{Attempt: *selectedAttempt, Operation: writerOperationRecheckBatch, Result: snapshot, ResultReady: true, Selection: recheckFailureSelection{Status: selectedJobStatus, QueueDepth: selectedQueueDepth, BatchSize: batchSize}, Completion: batchCompletion})
		return promoted, nil
	}, finalizer)
	if resultErr == nil && selectedAttempt != nil {
		s.clearPendingRecheckFailure(selectedAttempt.Token)
	}
	if resultErr != nil && selectedJobID == 0 {
		var queueBlocked interface {
			QueueBlocked() (string, *time.Time)
		}
		if errors.As(resultErr, &queueBlocked) {
			status, nextAttemptAt := queueBlocked.QueueBlocked()
			disposition := RecheckBatchQueueBlocked
			if status == "retry_wait" {
				disposition = RecheckBatchRetryDeferred
			}
			return RecheckBatchResult{Disposition: disposition, JobID: blockedJobID, Status: status, QueueDepth: blockedQueueDepth, NextAttemptAt: nextAttemptAt}, nil
		}
		return RecheckBatchResult{Disposition: RecheckBatchAcquisitionFailed}, resultErr
	}
	if resultErr != nil && selectedJobID != 0 {
		var commitOutcomeErr *writerCommitOutcomeError
		hasCommitOutcome := errors.As(resultErr, &commitOutcomeErr)
		commitNotCommitted := hasCommitOutcome && commitOutcomeErr.outcome == writerCommitOutcomeNotCommitted && selectedAttempt != nil
		if hasCommitOutcome && commitOutcomeErr.outcome == writerCommitOutcomeUnknown {
			failureClass, failureStage := "", recheckStageCommit
			if selectedAttempt != nil {
				selectedAttempt.FailureCause = commitOutcomeErr.OriginalCause()
				selectedAttempt.FailureStage = recheckStageCommit
				selectedAttempt.FailureClass = classifyRecheckFailure(selectedAttempt.FailureCause)
				selectedAttempt.AllowCommitted = true
				if pending := s.pendingRecheckFailureSnapshot(); pending != nil && pending.Attempt.Token == selectedAttempt.Token {
					pending.Attempt = *selectedAttempt
					s.setPendingRecheckFailure(*pending)
				}
				failureClass = string(selectedAttempt.FailureClass)
			}
			attrs := []slog.Attr{slog.String("writer_operation", writerOperationRecheckBatch), slog.Int64("job_id", selectedJobID), slog.String("status", selectedJobStatus), slog.Bool("queue_depth_available", selectedQueueDepth != nil), slog.Int("retry_count", selectedRetryCount), slog.String("stage", "commit_reconcile"), slog.Duration("duration", time.Since(startedAt)), slog.String("error", SanitizeRecheckError(resultErr))}
			if selectedQueueDepth != nil {
				attrs = append(attrs, slog.Int("queue_depth", *selectedQueueDepth))
			}
			runtimeLog.ErrorContext(ctx, "silence_recheck", "job_commit_outcome_unknown", "Silence recheck job commit outcome is unknown; job failure was not persisted", attrs...)
			result := selectedResult(RecheckBatchCommitOutcomeUnknown, "")
			result.FailureClass, result.FailureStage = failureClass, failureStage
			return result, resultErr
		}
		if selectedAttempt != nil {
			s.clearPendingRecheckFailure(selectedAttempt.Token)
		}
		if commitNotCommitted && expectedRecheckCancellationReason(ctx, resultErr) != "" {
			result := selectedResult(RecheckBatchInfrastructureFailed, "")
			result.FailureClass = expectedRecheckCancellationReason(ctx, resultErr)
			result.FailureStage = "batch"
			return result, resultErr
		}
		if commitNotCommitted {
			selectedAttempt.FailureCause = commitOutcomeErr.OriginalCause()
			selectedAttempt.FailureStage = recheckStageCommit
			selectedAttempt.FailureClass = classifyRecheckFailure(selectedAttempt.FailureCause)
			selectedAttempt.AllowCommitted = false
		} else if reason := recheckSelectedInfrastructureReason(ctx, resultErr); reason != "" {
			result := selectedResult(RecheckBatchInfrastructureFailed, "")
			result.FailureClass = reason
			result.FailureStage = "batch"
			return result, resultErr
		}
		if ctx.Err() != nil || context.Cause(ctx) != nil {
			result := selectedResult(RecheckBatchInfrastructureFailed, "")
			result.FailureClass = expectedRecheckCancellationReason(ctx, resultErr)
			if result.FailureClass == "" {
				result.FailureClass = "worker_cancelled"
			}
			result.FailureStage = "batch"
			return result, resultErr
		}
		if selectedAttempt == nil {
			return selectedResult(RecheckBatchAcquisitionFailed, ""), resultErr
		}
		if !commitNotCommitted {
			selectedAttempt.FailureCause = resultErr
			selectedAttempt.FailureStage = recheckFailureStage(resultErr)
			selectedAttempt.FailureClass = classifyRecheckFailure(resultErr)
		}
		pendingResult := selectedResult(RecheckBatchFailurePersistenceFailed, "")
		pendingResult.FailureClass, pendingResult.FailureStage = string(selectedAttempt.FailureClass), selectedAttempt.FailureStage
		s.setPendingRecheckFailure(recheckPendingFailure{Attempt: *selectedAttempt, Operation: writerOperationRecheckFailure, Result: pendingResult, Selection: recheckFailureSelection{Status: selectedJobStatus, QueueDepth: selectedQueueDepth, BatchSize: batchSize}, Completion: newRecheckFailureCompletion()})
		failureResult, persistErr := s.persistRecheckAttemptFailure(ctx, *selectedAttempt)
		if persistErr != nil {
			runtimeLog.ErrorContext(ctx, "silence_recheck", "failure_persistence_failed", "Silence recheck failure bookkeeping was not confirmed", slog.Int64("job_id", selectedJobID), slog.String("stage", recheckStagePersistFailure), slog.String("error", SanitizeRecheckError(persistErr)))
			result := selectedResult(RecheckBatchFailurePersistenceFailed, "")
			result.FailureClass, result.FailureStage = string(selectedAttempt.FailureClass), selectedAttempt.FailureStage
			return result, persistErr
		}
		if failureResult.RetryScheduled && failureResult.NextAttemptAt != nil {
			result := selectedResult(RecheckBatchRetryScheduled, "retry_wait")
			result.NextAttemptAt = failureResult.NextAttemptAt
			result.FailureClass, result.FailureStage = string(selectedAttempt.FailureClass), selectedAttempt.FailureStage
			if failureResult.Snapshot != nil {
				result = *failureResult.Snapshot
				result.Disposition = RecheckBatchRetryScheduled
				result.FailureClass, result.FailureStage = string(selectedAttempt.FailureClass), selectedAttempt.FailureStage
			}
			return result, &recheckRetryScheduledError{JobID: selectedJobID, NextAttemptAt: *failureResult.NextAttemptAt, Cause: resultErr}
		}
		result := selectedResult(RecheckBatchJobFailed, "failed")
		result.FailureClass, result.FailureStage = string(selectedAttempt.FailureClass), selectedAttempt.FailureStage
		if failureResult.Snapshot != nil {
			result = *failureResult.Snapshot
			result.Disposition = RecheckBatchJobFailed
			result.FailureClass, result.FailureStage = string(selectedAttempt.FailureClass), selectedAttempt.FailureStage
		}
		return result, NewRecheckDurableFailureError(selectedJobID, resultErr)
	}
	batchResult = RecheckBatchResult{Disposition: RecheckBatchCommitted, JobID: committedJobID, TriggerType: selectedTriggerType, Status: committedStatus, QueueDepth: selectedQueueDepth, CreatedAt: selectedCreatedAt, ProcessedCandidates: committedProcessed, TotalCandidates: committedTotalCandidates, OutcomeCounts: RecheckOutcomeCounts{Promoted: committedPromoted, Stale: committedStale, MissingLabels: committedMissing, Ineligible: committedIneligible, IncompleteSources: committedIncomplete}}
	if committedJobID == 0 {
		batchResult.Disposition = RecheckBatchQueueEmpty
	} else if committedResult != nil {
		batchResult = *committedResult
		batchResult.Disposition = RecheckBatchCommitted
	}
	return batchResult, resultErr
}

func logRecheckJobSelected(ctx context.Context, jobID int64, status string, queueDepth *int, attemptCount, retryCount, batchSize int) {
	attrs := []slog.Attr{
		slog.Int64("job_id", jobID),
		slog.String("status", status),
		slog.Int("attempt_count", attemptCount),
		slog.Int("retry_count", retryCount),
		slog.Int("batch_size", batchSize),
		slog.Bool("queue_depth_available", queueDepth != nil),
	}
	if queueDepth != nil {
		attrs = append(attrs, slog.Int("queue_depth", *queueDepth))
	}
	runtimeLog.Event(ctx, slog.LevelDebug, "silence_recheck", "job_selected", "Silence recheck job selected", attrs...)
}

func (s *Store) logRecheckBatchCompleted(ctx context.Context, jobID int64, processed, promoted, missingLabels, ineligible, incompleteSources, stale int, duration time.Duration, promotedAny bool) {
	level := slog.LevelDebug
	if promotedAny {
		level = slog.LevelInfo
	}
	runtimeLog.Event(ctx, level, "silence_recheck", "batch_completed", "Silence recheck batch completed", slog.Int64("job_id", jobID), slog.Int("processed", processed), slog.Int("promoted", promoted), slog.Int("stale", stale), slog.Int("missing_labels", missingLabels), slog.Int("ineligible", ineligible), slog.Int("incomplete_sources", incompleteSources), slog.Duration("duration", duration))
}

func recheckJobDuration(snapshot *RecheckBatchResult, fallback time.Time) time.Duration {
	if snapshot == nil || snapshot.StartedAt == nil {
		return time.Since(fallback)
	}
	if snapshot.FinishedAt != nil {
		return snapshot.FinishedAt.Sub(*snapshot.StartedAt)
	}
	return time.Since(*snapshot.StartedAt)
}

func expectedRecheckCancellationReason(ctx context.Context, err error) string {
	if errors.Is(err, context.DeadlineExceeded) || (ctx != nil && (errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(context.Cause(ctx), context.DeadlineExceeded))) {
		return "worker_deadline"
	}
	if errors.Is(err, context.Canceled) || (ctx != nil && (errors.Is(ctx.Err(), context.Canceled) || errors.Is(context.Cause(ctx), context.Canceled))) {
		return "worker_cancelled"
	}
	return ""
}

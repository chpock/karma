package alertlifecycle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	runtimeLog "github.com/prymitive/karma/internal/log"
)

const (
	recheckStageAcquireWriter    = "acquire_writer"
	recheckStageSelectJob        = "select_job"
	recheckStageLoadContext      = "load_context"
	recheckStageLoadCandidates   = "load_candidates"
	recheckStageEvaluate         = "evaluate"
	recheckStagePersistCandidate = "persist_candidate"
	recheckStageUpdateProgress   = "update_progress"
	recheckStageCommit           = "commit"
	recheckStagePersistFailure   = "persist_failure"
)

var errRecheckFailureFinalizing = errors.New("recheck failure finalization in progress")

var (
	ErrRecheckJobNotFound        = errors.New("recheck job not found")
	ErrRecheckJobStateConflict   = errors.New("recheck job state conflict")
	ErrRecheckOperatorConflict   = errors.New("recheck operator request conflict")
	ErrInvalidRecheckRequest     = errors.New("invalid recheck operator request")
	ErrRecheckFailurePersistence = errors.New("recheck failure persistence failed")
	ErrRecheckPreAttempt         = errors.New("recheck pre-attempt infrastructure failure")
)

type recheckPreAttemptError struct {
	reason    string
	retryable bool
	err       error
}

type recheckFailurePersistenceDisposition interface {
	ReconciliationResolved() bool
}

type recheckFailurePersistenceError struct {
	err                    error
	reconciliationResolved bool
	retryable              bool
}

func (e *recheckFailurePersistenceError) Error() string { return e.err.Error() }

func (e *recheckFailurePersistenceError) Unwrap() error { return e.err }

func (e *recheckFailurePersistenceError) ReconciliationResolved() bool {
	return e.reconciliationResolved
}

func (e *recheckFailurePersistenceError) Retryable() bool {
	return e.retryable
}

// RecheckFailurePersistenceReconciliationResolved reports whether the pending
// attempt's earlier commit reconciliation already reached a definitive result.
func RecheckFailurePersistenceReconciliationResolved(err error) bool {
	var disposition recheckFailurePersistenceDisposition
	return errors.As(err, &disposition) && disposition.ReconciliationResolved()
}

// IsRetryableRecheckFailurePersistence reports whether the failure-bookkeeping
// transaction failed for a retryable storage/commit reason. All untyped or
// invariant bookkeeping failures fail closed and are terminal.
func IsRetryableRecheckFailurePersistence(err error) bool {
	var disposition interface{ Retryable() bool }
	return errors.As(err, &disposition) && disposition.Retryable()
}

func NewRecheckFailurePersistenceError(cause error, retryable bool) error {
	resolved := false
	var reconciliation interface{ ReconciliationResolved() bool }
	if errors.As(cause, &reconciliation) {
		resolved = reconciliation.ReconciliationResolved()
	}
	return &recheckFailurePersistenceError{
		err:                    errors.Join(ErrRecheckFailurePersistence, cause),
		reconciliationResolved: resolved,
		retryable:              retryable,
	}
}

// RecheckDurableFailureError means the selected attempt was durably recorded
// as a failed FIFO head. It is a normal wakeable queue disposition, not a
// scheduler infrastructure failure.
type RecheckDurableFailureError struct {
	JobID int64
	Cause error
}

func (e *RecheckDurableFailureError) Error() string {
	if e == nil || e.Cause == nil {
		return "recheck job reached durable failed state"
	}
	return fmt.Sprintf("recheck job %d reached durable failed state: %v", e.JobID, e.Cause)
}

func (e *RecheckDurableFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewRecheckDurableFailureError(jobID int64, cause error) error {
	return &RecheckDurableFailureError{JobID: jobID, Cause: cause}
}

func IsRecheckDurableFailure(err error) bool {
	var durable *RecheckDurableFailureError
	return errors.As(err, &durable)
}

func NewRecheckWriterCommitReconciledError(operation string) error {
	return &writerCommitOutcomeError{outcome: writerCommitOutcomeCommitted, operation: operation, err: ErrWriterCommitReconciled}
}

func NewRecheckWriterCommitUnknownError(operation string, cause error) error {
	return &writerCommitOutcomeError{
		outcome:   writerCommitOutcomeUnknown,
		operation: operation,
		err:       errors.Join(ErrWriterCommitOutcomeUnknown, cause),
		cause:     cause,
	}
}

// RecheckPreAttemptDisposition is the scheduler-facing classification of an error that
// occurred after writer acquisition but before a durable attempt identity was created.
type RecheckPreAttemptDisposition interface {
	RecheckPreAttemptDisposition() (reason string, retryable bool)
}

func (e *recheckPreAttemptError) Error() string {
	return fmt.Sprintf("recheck pre-attempt infrastructure failure (%s): %v", e.reason, e.err)
}

func (e *recheckPreAttemptError) Unwrap() error { return e.err }

func (e *recheckPreAttemptError) Is(target error) bool {
	return target == ErrRecheckPreAttempt || errors.Is(e.err, target)
}

func (e *recheckPreAttemptError) RecheckPreAttemptDisposition() (string, bool) {
	return e.reason, e.retryable
}

func IsRetryableRecheckPreAttempt(err error) bool {
	var disposition RecheckPreAttemptDisposition
	if errors.As(err, &disposition) {
		_, retryable := disposition.RecheckPreAttemptDisposition()
		return retryable
	}
	return false
}

func IsRecheckPreAttemptError(err error) bool {
	var disposition RecheckPreAttemptDisposition
	return errors.As(err, &disposition)
}

func RecheckPreAttemptReason(err error) string {
	var disposition RecheckPreAttemptDisposition
	if errors.As(err, &disposition) {
		reason, _ := disposition.RecheckPreAttemptDisposition()
		return reason
	}
	return ""
}

func newRecheckPreAttemptError(reason string, err error) error {
	return &recheckPreAttemptError{reason: reason, retryable: classifyRecheckFailure(err) == recheckFailureRetryable, err: err}
}

type recheckStageError struct {
	stage string
	err   error
}

func (e *recheckStageError) Error() string { return fmt.Sprintf("%s: %v", e.stage, e.err) }

func (e *recheckStageError) Unwrap() error { return e.err }

func wrapRecheckStage(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &recheckStageError{stage: stage, err: err}
}

func recheckFailureStage(err error) string {
	var stageErr *recheckStageError
	if errors.As(err, &stageErr) {
		return stageErr.stage
	}
	return recheckStageEvaluate
}

type recheckAttemptInfo struct {
	JobID                       int64
	Token                       string
	Number                      int
	StartedAt                   time.Time
	RetryCountBefore            int
	ConsecutiveRetryCountBefore int
	FailureClass                recheckFailureClass
	FailureStage                string
	FailureCause                error
	AllowCommitted              bool
}

type recheckPendingFailure struct {
	Attempt     recheckAttemptInfo
	Operation   string
	Result      RecheckBatchResult
	ResultReady bool
	Selection   recheckFailureSelection
	Completion  *recheckFailureCompletion
}

type recheckOperatorFinalization struct {
	JobID      int64
	RequestID  string
	Completion *recheckFailureCompletion
}

type recheckFailureSelection struct {
	Status     string
	QueueDepth *int
	BatchSize  int
}

type recheckFailureResult struct {
	Status         string
	RetryScheduled bool
	NextAttemptAt  *time.Time
	Snapshot       *RecheckBatchResult
}

type recheckQueueBlockedError struct {
	JobID         int64
	Status        string
	NextAttemptAt *time.Time
}

func (e *recheckQueueBlockedError) QueueBlocked() (string, *time.Time) {
	return e.Status, e.NextAttemptAt
}

type RecheckQueueHead struct {
	JobID         int64
	Status        string
	NextAttemptAt *time.Time
	QueueDepth    int
}

func (s *Store) RecheckQueueHead(ctx context.Context) (RecheckQueueHead, error) {
	if s == nil || s.db == nil {
		return RecheckQueueHead{}, ErrStoreNotInitialized
	}
	var head RecheckQueueHead
	var next sql.NullString
	err := s.db.QueryRowContext(ctx, `
SELECT job_id,status,next_attempt_at,
       (SELECT COUNT(*) FROM silence_recheck_jobs WHERE status IN ('queued','running','retry_wait','failed'))
FROM silence_recheck_jobs
WHERE status IN ('queued','running','retry_wait','failed')
ORDER BY job_id ASC
LIMIT 1
`).Scan(&head.JobID, &head.Status, &next, &head.QueueDepth)
	if errors.Is(err, sql.ErrNoRows) {
		return RecheckQueueHead{}, nil
	}
	if err != nil {
		return RecheckQueueHead{}, err
	}
	if next.Valid {
		parsed, parseErr := time.Parse(time.RFC3339Nano, next.String)
		if parseErr != nil {
			return RecheckQueueHead{}, fmt.Errorf("%w: invalid recheck next_attempt_at: %w", ErrWriterOperationInvariant, parseErr)
		}
		head.NextAttemptAt = &parsed
	}
	return head, nil
}

func (e *recheckQueueBlockedError) Error() string {
	if e.NextAttemptAt != nil {
		return fmt.Sprintf("recheck job %d is %s until %s and blocks later jobs", e.JobID, e.Status, e.NextAttemptAt.UTC().Format(time.RFC3339Nano))
	}
	return fmt.Sprintf("recheck job %d is %s and blocks later jobs", e.JobID, e.Status)
}

type recheckRetryScheduledError struct {
	JobID         int64
	NextAttemptAt time.Time
	Cause         error
}

func (e *recheckRetryScheduledError) Error() string {
	return fmt.Sprintf("recheck job %d retry scheduled at %s: %v", e.JobID, e.NextAttemptAt.UTC().Format(time.RFC3339Nano), e.Cause)
}

func (e *recheckRetryScheduledError) Unwrap() error { return e.Cause }

func (e *recheckRetryScheduledError) NextTime() time.Time { return e.NextAttemptAt }

func (s *Store) createRecheckAttemptTx(tx *sql.Tx, jobID int64, startedAt time.Time) (recheckAttemptInfo, error) {
	var attemptCount int
	if err := tx.QueryRow(`SELECT attempt_count FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&attemptCount); err != nil {
		return recheckAttemptInfo{}, fmt.Errorf("failed to read recheck attempt count: %w", err)
	}
	var ledgerCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=?`, jobID).Scan(&ledgerCount); err != nil {
		return recheckAttemptInfo{}, fmt.Errorf("failed to read recheck attempt ledger count: %w", err)
	}
	if attemptCount != ledgerCount {
		return recheckAttemptInfo{}, fmt.Errorf("%w: job %d attempt_count=%d ledger_count=%d", ErrWriterOperationInvariant, jobID, attemptCount, ledgerCount)
	}
	token, err := s.nextWriterOperationToken()
	if err != nil {
		return recheckAttemptInfo{}, fmt.Errorf("failed to create recheck attempt token: %w", err)
	}
	return recheckAttemptInfo{JobID: jobID, Token: token, Number: attemptCount + 1, StartedAt: startedAt}, nil
}

func insertRecheckAttemptTx(tx *sql.Tx, attempt recheckAttemptInfo, outcome string, finishedAt time.Time, scheduledRetryAt *time.Time) error {
	var failureClass, failureStage, scheduled string
	if attempt.FailureClass != "" {
		failureClass = string(attempt.FailureClass)
	}
	if attempt.FailureStage != "" {
		failureStage = attempt.FailureStage
	}
	if scheduledRetryAt != nil {
		scheduled = scheduledRetryAt.UTC().Format(time.RFC3339Nano)
	}
	_, err := tx.Exec(`
INSERT INTO silence_recheck_job_attempts(
  job_id, attempt_token, attempt_number, started_at, finished_at, outcome,
  failure_class, failure_stage, scheduled_retry_at
) VALUES(?,?,?,?,?,?,?,?,?)
`, attempt.JobID, attempt.Token, attempt.Number, attempt.StartedAt.UTC().Format(time.RFC3339Nano), finishedAt.UTC().Format(time.RFC3339Nano), outcome, nullableString(failureClass), nullableString(failureStage), nullableString(scheduled))
	return err
}

func (s *Store) validateRecheckAttemptTupleLocked(ctx context.Context, attempt recheckAttemptInfo, expectedOutcome string, allowCommitted bool) error {
	if s.writerConn == nil {
		return fmt.Errorf("%w: writer connection is unavailable while validating attempt tuple: %w", ErrWriterCommitOutcomeUnknown, ErrWriterUnavailable)
	}
	jobID, attemptNumber, outcome, found, err := s.lookupRecheckAttemptTupleLocked(ctx, attempt.Token)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: attempt tuple is not durable for job %d/number %d", ErrWriterOperationInvariant, attempt.JobID, attempt.Number)
	}
	if jobID != attempt.JobID || attemptNumber != attempt.Number {
		return fmt.Errorf("%w: attempt tuple identifies job %d/number %d, want job %d/number %d", ErrWriterOperationInvariant, jobID, attemptNumber, attempt.JobID, attempt.Number)
	}
	validOutcome := outcome == expectedOutcome
	if allowCommitted && outcome == "committed" {
		validOutcome = true
	}
	if !validOutcome {
		return fmt.Errorf("%w: attempt tuple has outcome %q, want %q", ErrWriterOperationInvariant, outcome, expectedOutcome)
	}
	return nil
}

func (s *Store) validateRecheckAttemptAbsenceLocked(ctx context.Context, attempt recheckAttemptInfo) error {
	jobID, attemptNumber, outcome, found, err := s.lookupRecheckAttemptTupleLocked(ctx, attempt.Token)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("%w: writer token is absent but attempt tuple is durable for job %d/number %d/outcome %q", ErrWriterOperationInvariant, jobID, attemptNumber, outcome)
	}
	return nil
}

func (s *Store) lookupRecheckAttemptTupleLocked(ctx context.Context, attemptToken string) (int64, int, string, bool, error) {
	if s.writerConn == nil {
		return 0, 0, "", false, fmt.Errorf("%w: writer connection is unavailable while validating attempt tuple: %w", ErrWriterCommitOutcomeUnknown, ErrWriterUnavailable)
	}
	var jobID int64
	var attemptNumber int
	var outcome string
	recoveryCtx, cancel := newWriterRecoveryContext(ctx)
	defer cancel()
	var err error
	if s.recheckAttemptLookup != nil {
		jobID, attemptNumber, outcome, err = s.recheckAttemptLookup(recoveryCtx, attemptToken)
	} else {
		err = s.writerConn.QueryRowContext(recoveryCtx, `SELECT job_id,attempt_number,outcome FROM silence_recheck_job_attempts WHERE attempt_token=?`, attemptToken).Scan(&jobID, &attemptNumber, &outcome)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, "", false, nil
	}
	if err != nil {
		return 0, 0, "", false, fmt.Errorf("%w: attempt tuple lookup is unresolved: %w", ErrWriterCommitOutcomeUnknown, err)
	}
	return jobID, attemptNumber, outcome, true, nil
}

func (s *Store) commitRecheckAttemptTx(tx *sql.Tx, attempt recheckAttemptInfo, jobID int64, transition recheckJobState, finishedAt time.Time) error {
	if _, err := tx.Exec(`UPDATE silence_recheck_jobs SET status=?,attempt_count=?,consecutive_retry_count=?,next_attempt_at=?,finished_at=? WHERE job_id=?`, transition.Status, attempt.Number, transition.ConsecutiveRetryCount, nullableTime(transition.NextAttemptAt), nullableTime(transition.FinishedAt), jobID); err != nil {
		return err
	}
	return insertRecheckAttemptTx(tx, attempt, "committed", finishedAt, nil)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *Store) setPendingRecheckFailure(pending recheckPendingFailure) {
	if pending.Completion == nil {
		pending.Completion = newRecheckFailureCompletion()
	}
	s.recheckStateMu.Lock()
	s.pendingRecheckFailure = &pending
	s.recheckStateMu.Unlock()
}

func (s *Store) updatePendingRecheckResult(attemptToken, operation string, result RecheckBatchResult) {
	s.recheckStateMu.Lock()
	if s.pendingRecheckFailure != nil && s.pendingRecheckFailure.Attempt.Token == attemptToken && s.pendingRecheckFailure.Operation == operation {
		s.pendingRecheckFailure.Result = result
		s.pendingRecheckFailure.ResultReady = true
	}
	s.recheckStateMu.Unlock()
}

func (s *Store) pendingRecheckCompletion(attemptToken string) *recheckFailureCompletion {
	s.recheckStateMu.Lock()
	defer s.recheckStateMu.Unlock()
	if s.pendingRecheckFailure == nil || s.pendingRecheckFailure.Attempt.Token != attemptToken {
		return nil
	}
	return s.pendingRecheckFailure.Completion
}

func (s *Store) pendingTerminalRecheckFinalization(jobID int64) *recheckFailureCompletion {
	s.recheckStateMu.Lock()
	defer s.recheckStateMu.Unlock()
	pending := s.pendingRecheckFailure
	if pending == nil || pending.Attempt.JobID != jobID || pending.Operation != writerOperationRecheckFailure || !pending.ResultReady || pending.Result.Status != "failed" || pending.Completion == nil || !pending.Completion.isCommitted() || pending.Completion.isFinalized() {
		return nil
	}
	return pending.Completion
}

func (s *Store) pendingRecheckFailureSnapshot() *recheckPendingFailure {
	s.recheckStateMu.Lock()
	defer s.recheckStateMu.Unlock()
	if s.pendingRecheckFailure == nil {
		return nil
	}
	pending := *s.pendingRecheckFailure
	return &pending
}

func (s *Store) clearPendingRecheckFailure(attemptToken string) {
	s.recheckStateMu.Lock()
	if s.pendingRecheckFailure != nil && s.pendingRecheckFailure.Attempt.Token == attemptToken {
		s.pendingRecheckFailure = nil
	}
	s.recheckStateMu.Unlock()
}

func (s *Store) setPendingRecheckOperatorFinalization(pending recheckOperatorFinalization) {
	s.recheckStateMu.Lock()
	s.pendingRecheckOperator = &pending
	s.recheckStateMu.Unlock()
}

func (s *Store) pendingRecheckOperatorCompletion() *recheckFailureCompletion {
	s.recheckStateMu.Lock()
	defer s.recheckStateMu.Unlock()
	pending := s.pendingRecheckOperator
	if pending == nil || pending.Completion == nil || !pending.Completion.isCommitted() || pending.Completion.isFinalized() {
		return nil
	}
	return pending.Completion
}

func (s *Store) clearPendingRecheckOperatorFinalization(jobID int64, requestID string) {
	s.recheckStateMu.Lock()
	if s.pendingRecheckOperator != nil && s.pendingRecheckOperator.JobID == jobID && s.pendingRecheckOperator.RequestID == requestID {
		s.pendingRecheckOperator = nil
	}
	s.recheckStateMu.Unlock()
}

func (s *Store) disallowCommittedPendingRecheckAttempt(attemptToken string) {
	s.recheckStateMu.Lock()
	if s.pendingRecheckFailure != nil && s.pendingRecheckFailure.Attempt.Token == attemptToken {
		s.pendingRecheckFailure.Attempt.AllowCommitted = false
	}
	s.recheckStateMu.Unlock()
}

func (s *Store) persistRecheckAttemptFailure(ctx context.Context, attempt recheckAttemptInfo) (recheckFailureResult, error) {
	var result recheckFailureResult
	var expectedOutcome string
	completion := s.pendingRecheckCompletion(attempt.Token)
	if completion == nil {
		completion = newRecheckFailureCompletion()
	}
	finalizer := &writerAfterCommitFinalizer{
		ctx:           ctx,
		operationKind: writerOperationRecheckFailure,
		reconcile: func(writerCommitted bool) error {
			if expectedOutcome == "" {
				return fmt.Errorf("%w: failure outcome was not determined", ErrWriterOperationInvariant)
			}
			if !writerCommitted {
				if err := s.validateRecheckAttemptAbsenceLocked(ctx, attempt); err != nil {
					return err
				}
				attempt.AllowCommitted = false
				s.disallowCommittedPendingRecheckAttempt(attempt.Token)
				return nil
			}
			if err := s.validateRecheckAttemptTupleLocked(ctx, attempt, expectedOutcome, attempt.AllowCommitted); err != nil {
				return err
			}
			completion.completeCommitted()
			return nil
		},
		run: func(reconciled bool) {
			s.finalizeCommittedRecheckFailure(ctx, attempt.Token, reconciled)
		},
	}
	_, err := withWriterTxValueAfterCommit(ctx, s, writerOperationRecheckFailure, func(tx *sql.Tx) (struct{}, error) {
		if faultErr := s.recheckStageFault(recheckStagePersistFailure); faultErr != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, faultErr)
		}
		var status string
		var retryCount, consecutive, parentAttemptCount, ledgerCount int
		if err := tx.QueryRow(`SELECT status,retry_count,consecutive_retry_count,attempt_count FROM silence_recheck_jobs WHERE job_id=?`, attempt.JobID).Scan(&status, &retryCount, &consecutive, &parentAttemptCount); err != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, err)
		}
		if err := tx.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=?`, attempt.JobID).Scan(&ledgerCount); err != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, err)
		}
		if parentAttemptCount != ledgerCount {
			return struct{}{}, fmt.Errorf("%w: job %d attempt_count=%d ledger_count=%d", ErrWriterOperationInvariant, attempt.JobID, parentAttemptCount, ledgerCount)
		}
		now := s.currentRecheckTime()
		transitionState := recheckJobState{Status: "running", RetryCount: attempt.RetryCountBefore, ConsecutiveRetryCount: attempt.ConsecutiveRetryCountBefore}
		transitionEvent := recheckJobTerminalFailure
		if attempt.FailureClass == recheckFailureRetryable {
			transitionEvent = recheckJobRetryableFailure
		}
		transition, transitionErr := applyRecheckJobEvent(transitionState, transitionEvent, now)
		if transitionErr != nil {
			return struct{}{}, fmt.Errorf("%w: failure transition: %w", ErrWriterOperationInvariant, transitionErr)
		}
		expectedOutcome = "failed"
		if transition.Status == "retry_wait" {
			expectedOutcome = "retry_scheduled"
		}
		var existingJobID int64
		var existingOutcome string
		var existingNumber int
		var existingScheduled sql.NullString
		lookupErr := tx.QueryRow(`SELECT job_id,attempt_number,outcome,scheduled_retry_at FROM silence_recheck_job_attempts WHERE attempt_token=?`, attempt.Token).Scan(&existingJobID, &existingNumber, &existingOutcome, &existingScheduled)
		if lookupErr == nil {
			if existingJobID != attempt.JobID || existingNumber != attempt.Number {
				return struct{}{}, fmt.Errorf("%w: attempt tuple identifies job %d/number %d, want job %d/number %d", ErrWriterOperationInvariant, existingJobID, existingNumber, attempt.JobID, attempt.Number)
			}
			if existingOutcome == "committed" && attempt.AllowCommitted {
				result.Status = existingOutcome
				snapshot, snapshotErr := readRecheckJobResultTx(ctx, tx, attempt.JobID, false)
				if snapshotErr != nil {
					return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, snapshotErr)
				}
				result.Snapshot = &snapshot
				s.updatePendingRecheckResult(attempt.Token, writerOperationRecheckFailure, recheckBatchResultFromFailure(attempt, result))
				return struct{}{}, nil
			}
			if existingOutcome != expectedOutcome {
				return struct{}{}, fmt.Errorf("%w: attempt tuple has outcome %q, want %q", ErrWriterOperationInvariant, existingOutcome, expectedOutcome)
			}
			result.Status = "failed"
			result.RetryScheduled = existingOutcome == "retry_scheduled"
			if result.RetryScheduled {
				result.Status = "retry_wait"
				if !existingScheduled.Valid {
					return struct{}{}, fmt.Errorf("%w: retry attempt tuple has no schedule", ErrWriterOperationInvariant)
				}
				parsed, parseErr := time.Parse(time.RFC3339Nano, existingScheduled.String)
				if parseErr != nil {
					return struct{}{}, fmt.Errorf("%w: retry attempt tuple has invalid schedule: %w", ErrWriterOperationInvariant, parseErr)
				}
				result.NextAttemptAt = &parsed
			}
			snapshot, snapshotErr := readRecheckJobResultTx(ctx, tx, attempt.JobID, false)
			if snapshotErr != nil {
				return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, snapshotErr)
			}
			result.Snapshot = &snapshot
			s.updatePendingRecheckResult(attempt.Token, writerOperationRecheckFailure, recheckBatchResultFromFailure(attempt, result))
			return struct{}{}, nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, lookupErr)
		}
		if retryCount != attempt.RetryCountBefore || consecutive != attempt.ConsecutiveRetryCountBefore {
			return struct{}{}, fmt.Errorf("%w: job %d retry counters changed before failure bookkeeping: retry=%d/%d consecutive=%d/%d", ErrWriterOperationInvariant, attempt.JobID, retryCount, attempt.RetryCountBefore, consecutive, attempt.ConsecutiveRetryCountBefore)
		}
		if status == "completed" || status == "abandoned" {
			return struct{}{}, fmt.Errorf("%w: job %d is already %s", ErrRecheckJobStateConflict, attempt.JobID, status)
		}

		result.Status = transition.Status
		result.RetryScheduled = transition.Status == "retry_wait"
		var next *time.Time
		consecutive = transition.ConsecutiveRetryCount
		retryCount = transition.RetryCount
		if result.RetryScheduled {
			candidate := transition.NextAttemptAt
			next = candidate
		}
		result.NextAttemptAt = next
		finishedAt := now.Format(time.RFC3339Nano)
		nextRaw := nullableTime(next)
		finishedRaw := any(finishedAt)
		if result.Status == "retry_wait" {
			finishedRaw = nil
		}
		updateResult, err := tx.Exec(`
UPDATE silence_recheck_jobs
SET status=?, attempt_count=?, retry_count=?, consecutive_retry_count=?, next_attempt_at=?,
    started_at=COALESCE(started_at, ?),
    first_error_message=COALESCE(first_error_message, ?),
    first_error_class=COALESCE(first_error_class, ?),
    first_error_stage=COALESCE(first_error_stage, ?),
    first_error_at=COALESCE(first_error_at, ?),
    last_error_message=?, last_error_class=?, last_error_stage=?, last_error_at=?, finished_at=?
WHERE job_id=? AND status IN ('queued','running','retry_wait')
`, result.Status, attempt.Number, retryCount, consecutive, nextRaw, attempt.StartedAt.UTC().Format(time.RFC3339Nano),
			sanitizeRecheckFailureMessage(attempt.FailureCause), string(attempt.FailureClass), attempt.FailureStage, attempt.StartedAt.UTC().Format(time.RFC3339Nano),
			sanitizeRecheckFailureMessage(attempt.FailureCause), string(attempt.FailureClass), attempt.FailureStage, now.Format(time.RFC3339Nano), finishedRaw, attempt.JobID)
		if err != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, err)
		}
		rows, err := updateResult.RowsAffected()
		if err != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, err)
		}
		if rows != 1 {
			return struct{}{}, fmt.Errorf("%w: job %d was not in a mutable state", ErrRecheckJobStateConflict, attempt.JobID)
		}
		outcome := "failed"
		if result.RetryScheduled {
			outcome = "retry_scheduled"
		}
		if err = insertRecheckAttemptTx(tx, attempt, outcome, now, next); err != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, err)
		}
		snapshot, snapshotErr := readRecheckJobResultTx(ctx, tx, attempt.JobID, false)
		if snapshotErr != nil {
			return struct{}{}, wrapRecheckStage(recheckStagePersistFailure, snapshotErr)
		}
		result.Snapshot = &snapshot
		s.updatePendingRecheckResult(attempt.Token, writerOperationRecheckFailure, recheckBatchResultFromFailure(attempt, result))
		return struct{}{}, nil
	}, finalizer)
	if err != nil {
		return result, &recheckFailurePersistenceError{
			err:       errors.Join(ErrRecheckFailurePersistence, err),
			retryable: retryableRecheckFailurePersistence(err),
		}
	}
	return result, nil
}

func recheckBatchResultFromFailure(attempt recheckAttemptInfo, failure recheckFailureResult) RecheckBatchResult {
	result := RecheckBatchResult{JobID: attempt.JobID, Status: failure.Status, NextAttemptAt: failure.NextAttemptAt}
	if failure.Snapshot != nil {
		result = *failure.Snapshot
	}
	result.FailureClass = string(attempt.FailureClass)
	result.FailureStage = attempt.FailureStage
	switch result.Status {
	case "retry_wait":
		result.Disposition = RecheckBatchRetryScheduled
	case "failed":
		result.Disposition = RecheckBatchJobFailed
	default:
		result.Disposition = RecheckBatchCommitted
	}
	return result
}

func (s *Store) finalizeCommittedRecheckFailure(ctx context.Context, attemptToken string, _ bool) {
	pending := s.pendingRecheckFailureSnapshot()
	if pending == nil || pending.Attempt.Token != attemptToken || pending.Operation != writerOperationRecheckFailure || pending.Completion == nil {
		return
	}
	defer pending.Completion.completeFinalized()
	defer s.clearPendingRecheckFailure(attemptToken)
	s.signalRecheckFailureFinalizer()
	logRecheckJobSelected(ctx, pending.Result.JobID, pending.Selection.Status, pending.Selection.QueueDepth, pending.Result.AttemptCount, pending.Result.RetryCount, pending.Selection.BatchSize)
	if pending.Result.Status != "failed" {
		return
	}
	runtimeLog.ErrorContext(ctx, "silence_recheck", "job_failed", "Silence recheck job reached terminal failure", slog.Int64("job_id", pending.Result.JobID), slog.String("status", pending.Result.Status), slog.Int("attempt_count", pending.Result.AttemptCount), slog.Int("retry_count", pending.Result.RetryCount), slog.Int("consecutive_retry_count", pending.Result.ConsecutiveRetryCount), slog.String("failure_class", pending.Result.FailureClass), slog.String("failure_stage", pending.Result.FailureStage), slog.String("error", SanitizeRecheckError(pending.Attempt.FailureCause)))
}

func retryableRecheckFailurePersistence(err error) bool {
	if err == nil || errors.Is(err, ErrWriterOperationInvariant) || errors.Is(err, ErrWriterOperationToken) {
		return false
	}
	if IsWriterAcquisitionError(err) {
		return IsRetryableRecheckWriterAcquisition(err)
	}
	var outcome *writerCommitOutcomeError
	if errors.As(err, &outcome) {
		if outcome.outcome == writerCommitOutcomeUnknown {
			return false
		}
		return classifyRecheckFailure(outcome.OriginalCause()) == recheckFailureRetryable
	}
	if errors.Is(err, ErrWriterUnavailable) {
		return false
	}
	return IsRetryableRecheckStorageError(err)
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func sanitizeRecheckFailureMessage(err error) string {
	if err == nil {
		return "unknown recheck failure"
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 1024 {
		message = message[:1024]
	}
	return message
}

// SanitizeRecheckError exposes only a stable operational category to runtime
// logs. Attempt tokens, SQL text, and raw storage errors remain internal.
func SanitizeRecheckError(err error) string {
	if err == nil {
		return "unknown"
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrWriterOperationInvariant), errors.Is(err, ErrWriterOperationToken):
		return "writer_invariant"
	case errors.Is(err, ErrWriterUnavailable), IsWriterAcquisitionError(err):
		return "writer_unavailable"
	case errors.Is(err, ErrRecheckFailurePersistence):
		return "failure_persistence"
	default:
		return "storage_error"
	}
}

type RecheckOperatorActionResult struct {
	JobID         int64  `json:"jobId"`
	Action        string `json:"action"`
	RequestID     string `json:"requestId"`
	Applied       bool   `json:"applied"`
	CurrentStatus string `json:"currentStatus"`
}

type recheckOperatorFingerprintInput struct {
	FormatVersion    int     `json:"formatVersion"`
	Action           string  `json:"action"`
	NormalizedReason string  `json:"normalizedReason"`
	NormalizedActor  *string `json:"normalizedActor"`
}

func recheckOperatorFingerprint(action, reason string, actor *string) (string, error) {
	canonical := recheckOperatorFingerprintInput{FormatVersion: 1, Action: action, NormalizedReason: strings.TrimSpace(reason)}
	if actor != nil {
		trimmed := strings.TrimSpace(*actor)
		if trimmed != "" {
			canonical.NormalizedActor = &trimmed
		}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (s *Store) applyRecheckOperatorAction(ctx context.Context, jobID int64, action, requestID, reason string, actor *string, now time.Time) (RecheckOperatorActionResult, error) {
	if jobID <= 0 || (action != "retry" && action != "abandon") || !validRecheckRequestID(requestID) || len([]byte(strings.TrimSpace(reason))) > 1024 || (action == "abandon" && strings.TrimSpace(reason) == "") {
		return RecheckOperatorActionResult{}, ErrInvalidRecheckRequest
	}
	reason = strings.TrimSpace(reason)
	if actor != nil {
		trimmed := strings.TrimSpace(*actor)
		if trimmed == "" {
			actor = nil
		} else {
			actor = &trimmed
		}
	}
	fingerprint, err := recheckOperatorFingerprint(action, reason, actor)
	if err != nil {
		return RecheckOperatorActionResult{}, err
	}
	apply := func() (RecheckOperatorActionResult, error) {
		mutated := false
		eventReady := false
		var commitWake func()
		var eventJobID int64
		var eventAction, eventRequestID, eventStatus, eventOperator string
		completion := newRecheckFailureCompletion()
		finalizer := &writerAfterCommitFinalizer{
			ctx:           ctx,
			operationKind: writerOperationRecheckOperator,
			reconcile: func(writerCommitted bool) error {
				if !mutated {
					return nil
				}
				if writerCommitted {
					completion.completeCommitted()
				} else {
					s.clearPendingRecheckOperatorFinalization(eventJobID, eventRequestID)
				}
				return nil
			},
			handoff: func(writerHandoffDisposition) {
				if mutated {
					commitWake = s.recheckCommitWake
				}
			},
			run: func(bool) {
				if !mutated {
					return
				}
				if commitWake != nil {
					defer commitWake()
				}
				defer completion.completeFinalized()
				defer s.clearPendingRecheckOperatorFinalization(eventJobID, eventRequestID)
				s.signalRecheckOperatorFinalizer()
				if !eventReady {
					return
				}
				attrs := []slog.Attr{slog.Int64("job_id", eventJobID), slog.String("action", eventAction), slog.String("request_id", eventRequestID), slog.String("resulting_status", eventStatus)}
				if eventOperator != "" {
					attrs = append(attrs, slog.String("operator", eventOperator))
				}
				runtimeLog.Event(ctx, slog.LevelInfo, "silence_recheck", "operator_action_applied", "Silence recheck operator action applied", attrs...)
				eventReady = false
			},
		}
		return withWriterTxValueAfterCommit(ctx, s, writerOperationRecheckOperator, func(tx *sql.Tx) (RecheckOperatorActionResult, error) {
			if s.pendingTerminalRecheckFinalization(jobID) != nil {
				s.signalRecheckOperatorBarrier("inside_transaction")
				return RecheckOperatorActionResult{}, errRecheckFailureFinalizing
			}
			var recordedAction, recordedFingerprint, appliedResult, resultingStatus string
			lookupErr := tx.QueryRow(`SELECT action,request_fingerprint,COALESCE(applied_result,''),COALESCE(resulting_status,'') FROM silence_recheck_job_operator_actions WHERE job_id=? AND request_id=?`, jobID, requestID).Scan(&recordedAction, &recordedFingerprint, &appliedResult, &resultingStatus)
			if lookupErr == nil {
				if recordedAction != action || recordedFingerprint != fingerprint {
					return RecheckOperatorActionResult{}, ErrRecheckOperatorConflict
				}
				return RecheckOperatorActionResult{JobID: jobID, Action: action, RequestID: requestID, Applied: appliedResult == "applied", CurrentStatus: resultingStatus}, nil
			}
			if !errors.Is(lookupErr, sql.ErrNoRows) {
				return RecheckOperatorActionResult{}, lookupErr
			}
			var status string
			var retryCount, consecutiveRetryCount, recoveryCount int
			if statusErr := tx.QueryRow(`SELECT status,retry_count,consecutive_retry_count,recovery_count FROM silence_recheck_jobs WHERE job_id=?`, jobID).Scan(&status, &retryCount, &consecutiveRetryCount, &recoveryCount); statusErr != nil {
				if errors.Is(statusErr, sql.ErrNoRows) {
					return RecheckOperatorActionResult{}, ErrRecheckJobNotFound
				}
				return RecheckOperatorActionResult{}, statusErr
			}
			nowRaw := now.UTC().Format(time.RFC3339Nano)
			event := recheckJobOperatorRetry
			if action == "abandon" {
				event = recheckJobOperatorAbandon
			}
			transition, transitionErr := applyRecheckJobEvent(recheckJobState{Status: status, RetryCount: retryCount, ConsecutiveRetryCount: consecutiveRetryCount, RecoveryCount: recoveryCount}, event, now)
			if transitionErr != nil {
				return RecheckOperatorActionResult{}, fmt.Errorf("%w: job %d is %s", ErrRecheckJobStateConflict, jobID, status)
			}
			var abandonedAt, abandonedBy, abandonedReason any
			if action == "abandon" {
				abandonedAt = nowRaw
				abandonedReason = reason
				if actor != nil {
					abandonedBy = *actor
				}
			}
			_, err = tx.Exec(`
UPDATE silence_recheck_jobs
SET status=?, next_attempt_at=?, consecutive_retry_count=?, recovery_count=?,
    finished_at=?, abandoned_at=?, abandoned_by=?, abandon_reason=?
WHERE job_id=? AND status='failed'
`, transition.Status, nullableTime(transition.NextAttemptAt), transition.ConsecutiveRetryCount, transition.RecoveryCount, nullableTime(transition.FinishedAt), abandonedAt, abandonedBy, abandonedReason, jobID)
			if err != nil {
				return RecheckOperatorActionResult{}, err
			}
			var applied int64
			if err = tx.QueryRow(`SELECT changes()`).Scan(&applied); err != nil || applied != 1 {
				if err != nil {
					return RecheckOperatorActionResult{}, err
				}
				return RecheckOperatorActionResult{}, ErrRecheckJobStateConflict
			}
			if _, err = tx.Exec(`
INSERT INTO silence_recheck_job_operator_actions(job_id,request_id,action,actor,reason,request_fingerprint,requested_at,applied_at,applied_result,resulting_status)
VALUES(?,?,?,?,?,?,?,?,?,?)
`, jobID, requestID, action, actor, reason, fingerprint, nowRaw, nowRaw, "applied", transition.Status); err != nil {
				return RecheckOperatorActionResult{}, err
			}
			mutated = true
			eventReady = true
			eventJobID, eventAction, eventRequestID, eventStatus = jobID, action, requestID, transition.Status
			if actor != nil {
				eventOperator = *actor
			}
			s.setPendingRecheckOperatorFinalization(recheckOperatorFinalization{JobID: jobID, RequestID: requestID, Completion: completion})
			return RecheckOperatorActionResult{JobID: jobID, Action: action, RequestID: requestID, Applied: true, CurrentStatus: transition.Status}, nil
		}, finalizer)
	}
	for {
		if completion := s.pendingTerminalRecheckFinalization(jobID); completion != nil {
			if err := completion.waitFinalized(ctx); err != nil {
				return RecheckOperatorActionResult{}, err
			}
		}
		s.signalRecheckOperatorBarrier("after_precheck")
		result, applyErr := apply()
		if errors.Is(applyErr, errRecheckFailureFinalizing) {
			continue
		}
		if !errors.Is(applyErr, ErrWriterCommitReconciled) {
			return result, applyErr
		}
		reconciled, found, lookupErr := s.loadRecheckOperatorAction(ctx, jobID, action, requestID, fingerprint)
		if lookupErr != nil {
			return RecheckOperatorActionResult{}, lookupErr
		}
		if found {
			return reconciled, nil
		}
	}
}

func (s *Store) loadRecheckOperatorAction(ctx context.Context, jobID int64, action, requestID, fingerprint string) (RecheckOperatorActionResult, bool, error) {
	var recordedAction, recordedFingerprint, appliedResult, resultingStatus string
	err := s.db.QueryRowContext(ctx, `SELECT action,request_fingerprint,COALESCE(applied_result,''),COALESCE(resulting_status,'') FROM silence_recheck_job_operator_actions WHERE job_id=? AND request_id=?`, jobID, requestID).Scan(&recordedAction, &recordedFingerprint, &appliedResult, &resultingStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return RecheckOperatorActionResult{}, false, nil
	}
	if err != nil {
		return RecheckOperatorActionResult{}, false, err
	}
	if recordedAction != action || recordedFingerprint != fingerprint {
		return RecheckOperatorActionResult{}, false, ErrRecheckOperatorConflict
	}
	return RecheckOperatorActionResult{JobID: jobID, Action: action, RequestID: requestID, Applied: appliedResult == "applied", CurrentStatus: resultingStatus}, true, nil
}

func (s *Store) RetryRecheckJobContext(ctx context.Context, jobID int64, requestID, reason string, actor *string, now time.Time) (RecheckOperatorActionResult, error) {
	return s.applyRecheckOperatorAction(ctx, jobID, "retry", requestID, reason, actor, now)
}

func (s *Store) AbandonRecheckJobContext(ctx context.Context, jobID int64, requestID, reason string, actor *string, now time.Time) (RecheckOperatorActionResult, error) {
	return s.applyRecheckOperatorAction(ctx, jobID, "abandon", requestID, reason, actor, now)
}

func validRecheckRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == ':' || char == '-' {
			continue
		}
		return false
	}
	return true
}

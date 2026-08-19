package alertlifecycle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
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
	Attempt recheckAttemptInfo
}

type recheckFailureResult struct {
	Status         string
	RetryScheduled bool
	NextAttemptAt  *time.Time
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
		return fmt.Errorf("%w: writer connection is unavailable while validating attempt %q: %w", ErrWriterCommitOutcomeUnknown, attempt.Token, ErrWriterUnavailable)
	}
	jobID, attemptNumber, outcome, found, err := s.lookupRecheckAttemptTupleLocked(ctx, attempt.Token)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: attempt token %q is not durable", ErrWriterOperationInvariant, attempt.Token)
	}
	if jobID != attempt.JobID || attemptNumber != attempt.Number {
		return fmt.Errorf("%w: attempt token %q identifies job %d/number %d, want job %d/number %d", ErrWriterOperationInvariant, attempt.Token, jobID, attemptNumber, attempt.JobID, attempt.Number)
	}
	validOutcome := outcome == expectedOutcome
	if allowCommitted && outcome == "committed" {
		validOutcome = true
	}
	if !validOutcome {
		return fmt.Errorf("%w: attempt token %q has outcome %q, want %q", ErrWriterOperationInvariant, attempt.Token, outcome, expectedOutcome)
	}
	return nil
}

func (s *Store) validateRecheckAttemptAbsenceLocked(ctx context.Context, attempt recheckAttemptInfo) error {
	jobID, attemptNumber, outcome, found, err := s.lookupRecheckAttemptTupleLocked(ctx, attempt.Token)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("%w: writer token is absent but attempt token %q is durable for job %d/number %d/outcome %q", ErrWriterOperationInvariant, attempt.Token, jobID, attemptNumber, outcome)
	}
	return nil
}

func (s *Store) lookupRecheckAttemptTupleLocked(ctx context.Context, attemptToken string) (int64, int, string, bool, error) {
	if s.writerConn == nil {
		return 0, 0, "", false, fmt.Errorf("%w: writer connection is unavailable while validating attempt %q: %w", ErrWriterCommitOutcomeUnknown, attemptToken, ErrWriterUnavailable)
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
		return 0, 0, "", false, fmt.Errorf("%w: attempt token %q lookup is unresolved: %w", ErrWriterCommitOutcomeUnknown, attemptToken, err)
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

func (s *Store) setPendingRecheckFailure(attempt recheckAttemptInfo) {
	s.recheckStateMu.Lock()
	s.pendingRecheckFailure = &recheckPendingFailure{Attempt: attempt}
	s.recheckStateMu.Unlock()
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
			return s.validateRecheckAttemptTupleLocked(ctx, attempt, expectedOutcome, attempt.AllowCommitted)
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
		now := time.Now().UTC()
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
				return struct{}{}, fmt.Errorf("%w: attempt token %q identifies job %d/number %d, want job %d/number %d", ErrWriterOperationInvariant, attempt.Token, existingJobID, existingNumber, attempt.JobID, attempt.Number)
			}
			if existingOutcome == "committed" && attempt.AllowCommitted {
				result.Status = existingOutcome
				return struct{}{}, nil
			}
			if existingOutcome != expectedOutcome {
				return struct{}{}, fmt.Errorf("%w: attempt token %q has outcome %q, want %q", ErrWriterOperationInvariant, attempt.Token, existingOutcome, expectedOutcome)
			}
			result.Status = "failed"
			result.RetryScheduled = existingOutcome == "retry_scheduled"
			if result.RetryScheduled {
				result.Status = "retry_wait"
				if !existingScheduled.Valid {
					return struct{}{}, fmt.Errorf("%w: retry attempt token %q has no schedule", ErrWriterOperationInvariant, attempt.Token)
				}
				parsed, parseErr := time.Parse(time.RFC3339Nano, existingScheduled.String)
				if parseErr != nil {
					return struct{}{}, fmt.Errorf("%w: retry attempt token %q has invalid schedule: %w", ErrWriterOperationInvariant, attempt.Token, parseErr)
				}
				result.NextAttemptAt = &parsed
			}
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
		finalizer := &writerAfterCommitFinalizer{
			ctx:           ctx,
			operationKind: writerOperationRecheckOperator,
			handoff: func(writerHandoffDisposition) {
				if mutated {
					s.handoffRecheckCommitLocked()
				}
			},
		}
		return withWriterTxValueAfterCommit(ctx, s, writerOperationRecheckOperator, func(tx *sql.Tx) (RecheckOperatorActionResult, error) {
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
			return RecheckOperatorActionResult{JobID: jobID, Action: action, RequestID: requestID, Applied: true, CurrentStatus: transition.Status}, nil
		}, finalizer)
	}
	result, err := apply()
	if !errors.Is(err, ErrWriterCommitReconciled) {
		return result, err
	}
	reconciled, found, lookupErr := s.loadRecheckOperatorAction(ctx, jobID, action, requestID, fingerprint)
	if lookupErr != nil {
		return RecheckOperatorActionResult{}, lookupErr
	}
	if found {
		return reconciled, nil
	}
	return apply()
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

package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	moderncsqlite "modernc.org/sqlite"
)

const (
	recheckJobSelected         recheckJobEvent = "selected"
	recheckJobBatchCommitted   recheckJobEvent = "batch_committed"
	recheckJobBatchCompleted   recheckJobEvent = "batch_completed"
	recheckJobRetryableFailure recheckJobEvent = "retryable_failure"
	recheckJobTerminalFailure  recheckJobEvent = "terminal_failure"
	recheckJobOperatorRetry    recheckJobEvent = "operator_retry"
	recheckJobOperatorAbandon  recheckJobEvent = "operator_abandon"
)

type recheckJobEvent string

type recheckJobState struct {
	Status                string
	RetryCount            int
	ConsecutiveRetryCount int
	RecoveryCount         int
	NextAttemptAt         *time.Time
	FinishedAt            *time.Time
	AbandonedAt           *time.Time
}

type recheckFailureClass string

const (
	recheckFailureRetryable    recheckFailureClass = "retryable"
	recheckFailureTerminal     recheckFailureClass = "terminal"
	recheckFailureCancellation recheckFailureClass = "cancellation"
)

func recheckRetryDelay(consecutiveFailures int) (time.Duration, bool) {
	switch consecutiveFailures {
	case 0:
		return time.Second, true
	case 1:
		return 5 * time.Second, true
	case 2:
		return 30 * time.Second, true
	case 3:
		return 2 * time.Minute, true
	case 4:
		return 10 * time.Minute, true
	default:
		return 0, false
	}
}

func recheckQueueEligible(status string, nextAttemptAt *time.Time, now time.Time) bool {
	switch status {
	case "queued", "running":
		return true
	case "retry_wait":
		return nextAttemptAt != nil && !nextAttemptAt.After(now)
	default:
		return false
	}
}

func applyRecheckJobEvent(state recheckJobState, event recheckJobEvent, now time.Time) (recheckJobState, error) {
	if state.Status == "completed" || state.Status == "abandoned" {
		return state, fmt.Errorf("recheck job in terminal state %q cannot receive %s", state.Status, event)
	}
	switch event {
	case recheckJobSelected:
		if state.Status != "queued" && state.Status != "running" && state.Status != "retry_wait" {
			return state, fmt.Errorf("recheck job in state %q cannot be selected", state.Status)
		}
		state.Status = "running"
		state.NextAttemptAt = nil
		return state, nil
	case recheckJobBatchCommitted:
		if state.Status != "running" {
			return state, fmt.Errorf("recheck job in state %q cannot commit a batch", state.Status)
		}
		state.ConsecutiveRetryCount = 0
		state.NextAttemptAt = nil
		return state, nil
	case recheckJobBatchCompleted:
		if state.Status != "running" {
			return state, fmt.Errorf("recheck job in state %q cannot complete", state.Status)
		}
		state.Status = "completed"
		state.ConsecutiveRetryCount = 0
		state.NextAttemptAt = nil
		state.FinishedAt = timePtrValue(now)
		return state, nil
	case recheckJobRetryableFailure:
		if state.Status != "running" {
			return state, fmt.Errorf("recheck job in state %q cannot retry", state.Status)
		}
		state.ConsecutiveRetryCount++
		if delay, ok := recheckRetryDelay(state.ConsecutiveRetryCount - 1); ok {
			state.Status = "retry_wait"
			state.RetryCount++
			state.NextAttemptAt = timePtrValue(now.Add(delay))
			return state, nil
		}
		state.Status = "failed"
		state.NextAttemptAt = nil
		state.FinishedAt = timePtrValue(now)
		return state, nil
	case recheckJobTerminalFailure:
		if state.Status != "running" {
			return state, fmt.Errorf("recheck job in state %q cannot fail", state.Status)
		}
		state.Status = "failed"
		state.NextAttemptAt = nil
		state.FinishedAt = timePtrValue(now)
		return state, nil
	case recheckJobOperatorRetry:
		if state.Status != "failed" {
			return state, fmt.Errorf("recheck job in state %q cannot be retried by operator", state.Status)
		}
		state.Status = "queued"
		state.ConsecutiveRetryCount = 0
		state.NextAttemptAt = nil
		state.FinishedAt = nil
		state.AbandonedAt = nil
		state.RecoveryCount++
		return state, nil
	case recheckJobOperatorAbandon:
		if state.Status != "failed" {
			return state, fmt.Errorf("recheck job in state %q cannot be abandoned", state.Status)
		}
		state.Status = "abandoned"
		state.NextAttemptAt = nil
		state.FinishedAt = timePtrValue(now)
		state.AbandonedAt = timePtrValue(now)
		return state, nil
	default:
		return state, fmt.Errorf("unknown recheck job event %q", event)
	}
}

func timePtrValue(value time.Time) *time.Time {
	valueCopy := value
	return &valueCopy
}

func isRetryableRecheckSQLiteCode(code int) bool {
	switch code {
	case 5, 261, 517, 773:
		return true
	default:
		return false
	}
}

func classifyRecheckFailure(err error) recheckFailureClass {
	if err == nil {
		return recheckFailureTerminal
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return recheckFailureCancellation
	}
	var sqliteErr *moderncsqlite.Error
	if errors.As(err, &sqliteErr) && isRetryableRecheckSQLiteCode(sqliteErr.Code()) {
		return recheckFailureRetryable
	}
	var codedErr interface{ SQLiteCode() int }
	if errors.As(err, &codedErr) && isRetryableRecheckSQLiteCode(codedErr.SQLiteCode()) {
		return recheckFailureRetryable
	}
	return recheckFailureTerminal
}

func IsRetryableRecheckWriterAcquisition(err error) bool {
	if errors.Is(err, ErrWriterOperationInvariant) || errors.Is(err, ErrWriterOperationToken) {
		return false
	}
	var acquisitionErr *writerAcquisitionError
	if errors.As(err, &acquisitionErr) {
		if errors.Is(acquisitionErr.err, driver.ErrBadConn) || errors.Is(acquisitionErr.err, sql.ErrConnDone) {
			return true
		}
		return classifyRecheckFailure(acquisitionErr.err) == recheckFailureRetryable
	}
	if errors.Is(err, ErrWriterAcquisition) && (errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone)) {
		return true
	}
	return errors.Is(err, ErrWriterAcquisition) && classifyRecheckFailure(err) == recheckFailureRetryable
}

func IsRetryableRecheckStorageError(err error) bool {
	return classifyRecheckFailure(err) == recheckFailureRetryable
}

func IsWriterAcquisitionError(err error) bool {
	var acquisitionErr *writerAcquisitionError
	return errors.As(err, &acquisitionErr) || errors.Is(err, ErrWriterAcquisition)
}

package alertlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

type problem3Review5CodedError struct{ code int }

func (e problem3Review5CodedError) Error() string { return "review5 coded storage error" }

func (e problem3Review5CodedError) SQLiteCode() int { return e.code }

func TestProblem3Review5FailurePersistenceClassificationMatrix(t *testing.T) {
	for name, test := range map[string]struct {
		failure       error
		failToken     bool
		wantRetryable bool
	}{
		"retryable_busy":   {failure: problem3Review5CodedError{code: 5}, wantRetryable: true},
		"invariant":        {failure: ErrWriterOperationInvariant},
		"non_busy_sql":     {failure: problem3Review5CodedError{code: 13}},
		"token_generation": {failure: errors.New("writer token entropy unavailable"), failToken: true},
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			before := captureProblem3Review2State(t, store.db)
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return errors.New("review5 selected candidate failure")
				}
				if stage == recheckStagePersistFailure && !test.failToken {
					return test.failure
				}
				return nil
			}}
			if test.failToken {
				calls := 0
				store.writerOperationToken = func() (string, error) {
					calls++
					// The selected batch operation and its durable attempt use the first
					// two requests; failure bookkeeping is the next operation request.
					if calls == 3 {
						return "", errors.New("writer token entropy unavailable")
					}
					return newWriterOperationToken()
				}
			}
			_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if err == nil || !errors.Is(err, ErrRecheckFailurePersistence) {
				t.Fatalf("failure persistence result=%v", err)
			}
			if test.failToken && !errors.Is(err, ErrWriterOperationToken) {
				t.Fatalf("token-generation case did not reach the second operation token request: %v", err)
			}
			if got := IsRetryableRecheckFailurePersistence(err); got != test.wantRetryable {
				t.Fatalf("retryable=%v want=%v err=%v", got, test.wantRetryable, err)
			}
			if after := captureProblem3Review2State(t, store.db); after != before {
				t.Fatal("failure-bookkeeping error changed durable attempt or domain state")
			}
		})
	}
}

func TestProblem3Review5RetainedFailedBookkeepingLogsJobFailedOnce(t *testing.T) {
	var logOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return errors.New("review5 selected terminal failure")
		}
		return nil
	}}
	commitCalls := 0
	store.writerCommit = func(tx *sql.Tx) error {
		commitCalls++
		if err := tx.Commit(); err != nil {
			return err
		}
		if commitCalls == 1 {
			return errors.New("review5 failure bookkeeping commit uncertain")
		}
		return nil
	}
	lookupCalls := 0
	store.writerCommitLookup = func(context.Context, string, string) (bool, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return false, context.DeadlineExceeded
		}
		return true, nil
	}
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !errors.Is(err, ErrWriterCommitOutcomeUnknown) || !errors.Is(err, ErrRecheckFailurePersistence) {
		t.Fatalf("initial unknown bookkeeping=%v", err)
	}
	store.writerCommit = nil
	if _, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000); !IsRecheckDurableFailure(err) {
		t.Fatalf("durable failed-head replay=%v", err)
	}
	if got := strings.Count(logOutput.String(), `"event":"job_failed"`); got != 1 {
		t.Fatalf("job_failed log count=%d output=%s", got, logOutput.String())
	}
}

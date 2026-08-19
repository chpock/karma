package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func TestProblem3Review7FailureBookkeepingAcquisitionPrecedence(t *testing.T) {
	for name, test := range map[string]struct {
		beginErr      error
		wantRetryable bool
	}{
		"bad_connection":  {beginErr: driver.ErrBadConn, wantRetryable: true},
		"connection_done": {beginErr: sql.ErrConnDone, wantRetryable: true},
		"sqlite_busy":     {beginErr: problem3Review5CodedError{code: 5}, wantRetryable: true},
		"sqlite_full":     {beginErr: problem3Review5CodedError{code: 13}},
		"sqlite_corrupt":  {beginErr: problem3Review5CodedError{code: 11}},
		"unknown":         {beginErr: errors.New("review7 unknown acquisition failure")},
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					return problem3SQLiteError(5)
				}
				return nil
			}}
			store.writerTransactionStarter = &review6SequencedTransactionStarter{
				inner:  store.writerTransactionStarter,
				failAt: 2,
				err:    test.beginErr,
			}
			store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
				return nil, errors.New("review7 replacement acquisition unavailable")
			}

			_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if err == nil || !errors.Is(err, ErrRecheckFailurePersistence) || !IsWriterAcquisitionError(err) {
				t.Fatalf("acquisition result=%v", err)
			}
			if got := IsRetryableRecheckFailurePersistence(err); got != test.wantRetryable {
				t.Fatalf("retryable=%v want=%v err=%v", got, test.wantRetryable, err)
			}
		})
	}
}

func TestProblem3Review7FirstReadinessFailureRetainsPendingAttempt(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		if stage == recheckStagePersistCandidate {
			return problem3SQLiteError(5)
		}
		return nil
	}}
	store.writerTransactionStarter = &review6SequencedTransactionStarter{
		inner:  store.writerTransactionStarter,
		failAt: 2,
		err:    driver.ErrBadConn,
	}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
		return nil, errors.Join(driver.ErrBadConn, errors.New("review7 replacement unavailable"))
	}

	_, initialErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if initialErr == nil || !IsRetryableRecheckFailurePersistence(initialErr) {
		t.Fatalf("initial pending failure=%v", initialErr)
	}
	pendingBefore := store.pendingRecheckFailureSnapshot()
	if pendingBefore == nil {
		t.Fatal("initial failure did not retain pending attempt")
	}

	_, readinessErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if readinessErr == nil || !IsWriterAcquisitionError(readinessErr) || !IsRetryableRecheckWriterAcquisition(readinessErr) {
		t.Fatalf("first readiness acquisition=%v", readinessErr)
	}
	pendingAfter := store.pendingRecheckFailureSnapshot()
	if pendingAfter == nil || pendingAfter.Attempt.Token != pendingBefore.Attempt.Token {
		t.Fatalf("first readiness failure changed pending attempt: before=%+v after=%+v", pendingBefore, pendingAfter)
	}

	store.writerConnectionAcquirer = store.db.Conn
	_, retryErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	var retryScheduled *recheckRetryScheduledError
	if !errors.As(retryErr, &retryScheduled) {
		t.Fatalf("bookkeeping recovery=%v", retryErr)
	}
	if store.pendingRecheckFailureSnapshot() != nil {
		t.Fatal("pending failure remained after successful recovery")
	}
	var attempts int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("first readiness recovery created duplicate attempt: %d", attempts)
	}
}

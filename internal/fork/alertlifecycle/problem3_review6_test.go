package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

type review6SequencedTransactionStarter struct {
	inner  writerTransactionStarter
	calls  int
	failAt int
	err    error
}

func (s *review6SequencedTransactionStarter) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	s.calls++
	if s.calls == s.failAt {
		return nil, s.err
	}
	return s.inner.BeginTx(ctx, opts)
}

func TestProblem3Review6FailureBookkeepingAcquisitionIsRetryable(t *testing.T) {
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
		return nil, errors.New("review6 replacement acquisition unavailable")
	}

	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err == nil || !errors.Is(err, ErrRecheckFailurePersistence) {
		t.Fatalf("failure-bookkeeping acquisition result=%v", err)
	}
	if !IsWriterAcquisitionError(err) {
		t.Fatalf("failure-bookkeeping acquisition lost its typed origin: %v", err)
	}
	if !IsRetryableRecheckFailurePersistence(err) {
		t.Fatalf("recoverable failure-bookkeeping acquisition was classified terminal: %v", err)
	}
	if store.pendingRecheckFailureSnapshot() == nil {
		t.Fatal("pending failure was not retained after acquisition failure")
	}
	store.writerConnectionAcquirer = store.db.Conn
	_, retryErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	var retryScheduled *recheckRetryScheduledError
	if !errors.As(retryErr, &retryScheduled) {
		t.Fatalf("bookkeeping retry after writer recovery=%v", retryErr)
	}
	if store.pendingRecheckFailureSnapshot() != nil {
		t.Fatal("pending failure remained after successful bookkeeping retry")
	}
	var attempts int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM silence_recheck_job_attempts WHERE job_id=1`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("writer recovery started another attempt: attempts=%d", attempts)
	}
}

func TestProblem3Review6FailureBookkeepingSQLConnDoneIsRetryable(t *testing.T) {
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
		err:    sql.ErrConnDone,
	}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
		return nil, errors.New("review6 replacement acquisition unavailable")
	}

	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err == nil || !errors.Is(err, ErrRecheckFailurePersistence) || !IsWriterAcquisitionError(err) {
		t.Fatalf("SQL connection-done acquisition result=%v", err)
	}
	if !IsRetryableRecheckFailurePersistence(err) {
		t.Fatalf("SQL connection-done acquisition was classified terminal: %v", err)
	}
}

func TestProblem3Review6TerminalCallbackWithUnavailableCleanupStaysTerminal(t *testing.T) {
	store, _, _ := seedProblem2OpenJob(t)
	t.Cleanup(func() { _ = store.Close() })
	store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
		switch stage {
		case recheckStagePersistCandidate:
			return problem3SQLiteError(5)
		case recheckStagePersistFailure:
			return errors.New("review6 terminal bookkeeping callback")
		default:
			return nil
		}
	}}
	rollbackCalls := 0
	store.writerRollback = func(tx *sql.Tx) error {
		rollbackCalls++
		rollbackErr := tx.Rollback()
		if rollbackCalls == 2 {
			return errors.New("review6 rollback failed")
		}
		return rollbackErr
	}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
		return nil, errors.New("review6 cleanup replacement unavailable")
	}

	_, err := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
	if err == nil || !errors.Is(err, ErrRecheckFailurePersistence) || !errors.Is(err, ErrWriterUnavailable) {
		t.Fatalf("terminal callback cleanup result=%v", err)
	}
	if IsRetryableRecheckFailurePersistence(err) {
		t.Fatalf("terminal callback was made retryable by cleanup failure: %v", err)
	}
	if store.pendingRecheckFailureSnapshot() == nil {
		t.Fatal("terminal bookkeeping failure did not retain pending failure")
	}
}

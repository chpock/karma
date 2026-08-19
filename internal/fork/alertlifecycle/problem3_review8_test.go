package alertlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
)

func review8SameAttempt(a, b recheckAttemptInfo) bool {
	return a.JobID == b.JobID &&
		a.Token == b.Token &&
		a.Number == b.Number &&
		a.StartedAt.Equal(b.StartedAt) &&
		a.RetryCountBefore == b.RetryCountBefore &&
		a.ConsecutiveRetryCountBefore == b.ConsecutiveRetryCountBefore &&
		a.FailureClass == b.FailureClass &&
		a.FailureStage == b.FailureStage &&
		review8ErrorText(a.FailureCause) == review8ErrorText(b.FailureCause) &&
		a.AllowCommitted == b.AllowCommitted
}

func review8ErrorText(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T:%v", err, err)
}

func TestProblem3Review8FirstReadinessReplacementCausePrecedenceAndNoEffects(t *testing.T) {
	for name, test := range map[string]struct {
		replacementErr error
		wantRetryable  bool
	}{
		"busy":    {replacementErr: problem3Review5CodedError{code: 5}, wantRetryable: true},
		"full":    {replacementErr: problem3Review5CodedError{code: 13}},
		"corrupt": {replacementErr: problem3Review5CodedError{code: 11}},
		"unknown": {replacementErr: errors.New("review8 unknown replacement cause")},
	} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := seedProblem2OpenJob(t)
			t.Cleanup(func() { _ = store.Close() })
			candidateCalls := 0
			store.lifecycleTestHooks = &storeLifecycleTestHooks{recheckFault: func(stage string) error {
				if stage == recheckStagePersistCandidate {
					candidateCalls++
					return problem3SQLiteError(5)
				}
				return nil
			}}
			starter := &review6SequencedTransactionStarter{
				inner:  store.writerTransactionStarter,
				failAt: 2,
				err:    driver.ErrBadConn,
			}
			store.writerTransactionStarter = starter
			store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
				return nil, test.replacementErr
			}
			callbackCalls := 0
			store.writerTransactionFinished = func(string) { callbackCalls++ }

			_, initialErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if initialErr == nil || !IsRetryableRecheckFailurePersistence(initialErr) {
				t.Fatalf("initial pending failure=%v", initialErr)
			}
			pendingBefore := store.pendingRecheckFailureSnapshot()
			if pendingBefore == nil {
				t.Fatal("initial failure did not retain pending attempt")
			}
			beforeState := captureProblem3Review2State(t, store.db)
			beforeStarterCalls := starter.calls
			beforeCallbackCalls := callbackCalls
			beforeCandidateCalls := candidateCalls

			_, readinessErr := store.RunSilenceRecheckBatchContext(context.Background(), 1000)
			if readinessErr == nil || !IsWriterAcquisitionError(readinessErr) {
				t.Fatalf("first readiness result=%v", readinessErr)
			}
			if got := IsRetryableRecheckWriterAcquisition(readinessErr); got != test.wantRetryable {
				t.Fatalf("first readiness retryable=%v want=%v err=%v", got, test.wantRetryable, readinessErr)
			}
			if afterState := captureProblem3Review2State(t, store.db); afterState != beforeState {
				t.Fatal("failed first readiness changed durable state")
			}
			pendingAfter := store.pendingRecheckFailureSnapshot()
			if pendingAfter == nil || !review8SameAttempt(pendingAfter.Attempt, pendingBefore.Attempt) {
				t.Fatalf("failed first readiness changed complete pending attempt: before=%+v after=%+v", pendingBefore, pendingAfter)
			}
			if starter.calls != beforeStarterCalls {
				t.Fatalf("failed first readiness entered BeginTx: before=%d after=%d", beforeStarterCalls, starter.calls)
			}
			if callbackCalls != beforeCallbackCalls {
				t.Fatalf("failed first readiness entered writer callback: before=%d after=%d", beforeCallbackCalls, callbackCalls)
			}
			if candidateCalls != beforeCandidateCalls {
				t.Fatalf("failed first readiness executed candidate seam: before=%d after=%d", beforeCandidateCalls, candidateCalls)
			}
		})
	}
}

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestRecheckSchedulerWakeIsCapacityOne(t *testing.T) {
	scheduler := newRecheckScheduler(nil, nil)
	scheduler.Signal()
	scheduler.Signal()
	if got := len(scheduler.wake); got != 1 {
		t.Fatalf("wake channel length=%d want=1", got)
	}
}

func TestProblem3Review4WakeCannotBypassProcessLocalBackoff(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	calls := make(chan int, 4)
	signalDeferred := make(chan struct{}, 1)
	wakeDeferred := make(chan struct{}, 1)
	scheduler.signalDeferred = func() { signalDeferred <- struct{}{} }
	scheduler.wakeDeferred = func() { wakeDeferred <- struct{}{} }
	call := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		calls <- call
		if call == 1 {
			return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5))
		}
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	if got := <-calls; got != 1 {
		t.Fatalf("initial runner call=%d", got)
	}
	if got := <-clock.timer.resetEvents; got != time.Second {
		t.Fatalf("backoff=%s want=1s", got)
	}
	scheduler.stateMu.Lock()
	timerActive, timerKind, timerOwner := scheduler.timerActive, scheduler.timerKind, scheduler.timerOwner
	scheduler.stateMu.Unlock()
	if !timerActive || timerKind != recheckSchedulerTimerBackoff || timerOwner != recheckBackoffAcquisition {
		t.Fatalf("process-local timer state active=%v kind=%d owner=%q", timerActive, timerKind, timerOwner)
	}
	scheduler.Signal()
	<-signalDeferred
	<-wakeDeferred
	select {
	case got := <-calls:
		t.Fatalf("wake bypassed process-local backoff with runner call=%d", got)
	default:
	}
	clock.timer.ch <- clock.now.Add(time.Second)
	if got := <-calls; got != 2 {
		t.Fatalf("timer runner call=%d want=2", got)
	}
	scheduler.Stop()
}

func TestProblem3Review4WakeCannotBypassPersistedRetryDueTimer(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	calls := make(chan int, 4)
	signalDeferred := make(chan struct{}, 1)
	wakeDeferred := make(chan struct{}, 1)
	scheduler.signalDeferred = func() { signalDeferred <- struct{}{} }
	scheduler.wakeDeferred = func() { wakeDeferred <- struct{}{} }
	call := 0
	next := clock.now.Add(30 * time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		calls <- call
		if call == 1 {
			return alertlifecycle.RecheckBatchResult{}, &alertlifecycleTestRetryScheduledError{next: next}
		}
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	if got := <-calls; got != 1 {
		t.Fatalf("initial runner call=%d", got)
	}
	if got := <-clock.timer.resetEvents; got != 30*time.Second {
		t.Fatalf("persisted due timer=%s want=30s", got)
	}
	scheduler.stateMu.Lock()
	timerActive, timerKind := scheduler.timerActive, scheduler.timerKind
	scheduler.stateMu.Unlock()
	if !timerActive || timerKind != recheckSchedulerTimerPersistedDue {
		t.Fatalf("persisted timer state active=%v kind=%d", timerActive, timerKind)
	}
	scheduler.Signal()
	<-signalDeferred
	<-wakeDeferred
	select {
	case got := <-calls:
		t.Fatalf("wake bypassed persisted due timer with runner call=%d", got)
	default:
	}
	clock.now = next
	clock.timer.ch <- next
	if got := <-calls; got != 2 {
		t.Fatalf("timer runner call=%d want=2", got)
	}
	scheduler.Stop()
}

func TestProblem3Review4EmptyStoreRunDoesNotSelfWake(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(store, clock)
	calls := make(chan int, 4)
	productionRunner := scheduler.runner
	call := 0
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		calls <- call
		return productionRunner(ctx, batchSize)
	}
	store.SetRecheckCommitWake(scheduler.Signal)
	store.SetRecheckReconciledWake(scheduler.SignalReconciled)
	scheduler.Start()
	if got := <-calls; got != 1 {
		t.Fatalf("empty-store runner call=%d", got)
	}
	select {
	case got := <-calls:
		t.Fatalf("empty-store commit self-woke runner call=%d", got)
	default:
	}
	scheduler.Stop()
}

func TestProblem3Review4ReconciledHandoffWakesIdleSchedulerExactlyOnce(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	calls := make(chan int, 4)
	call := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		calls <- call
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	if got := <-calls; got != 1 {
		t.Fatalf("startup runner call=%d", got)
	}
	scheduler.SignalReconciled()
	if got := <-calls; got != 2 {
		t.Fatalf("reconciled wake runner call=%d want=2", got)
	}
	select {
	case got := <-calls:
		t.Fatalf("reconciled handoff generated duplicate wake runner call=%d", got)
	default:
	}
	scheduler.Stop()
}

func TestProblem3Review4CommitUnknownResetsPreAttemptOnlyAndAdvancesReconciliation(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	scheduler.backoffStep[recheckBackoffPreAttempt] = 2
	scheduler.backoffStep[recheckBackoffReconciliation] = 2
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckWriterCommitUnknownError(alertlifecycle.RecheckBatchWriterOperation, context.DeadlineExceeded)
	}
	timer := clock.NewTimer(time.Second)
	scheduler.process(timer, nil)
	if _, ok := scheduler.backoffStep[recheckBackoffPreAttempt]; ok {
		t.Fatal("commit uncertainty retained pre-attempt backoff")
	}
	if got := scheduler.backoffStep[recheckBackoffReconciliation]; got != 3 {
		t.Fatalf("reconciliation step=%d want=3", got)
	}
}

type problem3Review4ResolvedBookkeepingError struct{}

func (problem3Review4ResolvedBookkeepingError) Error() string {
	return "resolved reconciliation bookkeeping failure"
}

func (problem3Review4ResolvedBookkeepingError) ReconciliationResolved() bool { return true }

func TestProblem3Review4ResolvedReconciliationThenBookkeepingFailureUsesBookkeepingOwner(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	scheduler.backoffStep[recheckBackoffReconciliation] = 2
	scheduler.backoffStep[recheckBackoffBookkeeping] = 2
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckFailurePersistenceError(problem3Review4ResolvedBookkeepingError{}, true)
	}
	timer := clock.NewTimer(time.Second)
	scheduler.process(timer, nil)
	if _, ok := scheduler.backoffStep[recheckBackoffReconciliation]; ok {
		t.Fatal("definitive reconciliation retained reconciliation backoff")
	}
	if got := scheduler.backoffStep[recheckBackoffBookkeeping]; got != 3 {
		t.Fatalf("bookkeeping step=%d want=3", got)
	}
}

func TestProblem3Review4QueueReadBackoffSurvivesRunnerDisposition(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, schedulerSQLiteError(5)
	}
	scheduler.process(timer, nil)
	scheduler.process(timer, nil)
	if got := scheduler.backoffStep[recheckBackoffQueueRead]; got != 2 {
		t.Fatalf("queue-read step=%d want=2", got)
	}
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, &alertlifecycleTestRetryScheduledError{next: clock.now.Add(time.Minute)}
	}
	scheduler.process(timer, nil)
	if got := scheduler.backoffStep[recheckBackoffQueueRead]; got != 2 {
		t.Fatalf("retry disposition reset queue-read step=%d want=2", got)
	}
}

func TestRecheckSchedulerStartsOneOwnerAndStopsIdempotently(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	scheduler := newRecheckScheduler(store, nil)
	scheduler.Start()
	scheduler.Signal()
	scheduler.Stop()
	scheduler.Stop()
	select {
	case <-scheduler.done:
	default:
		t.Fatal("scheduler did not stop")
	}
}

func TestRecheckSchedulerAcquisitionBackoff(t *testing.T) {
	for _, test := range []struct {
		step int
		want time.Duration
	}{
		{step: 1, want: time.Second},
		{step: 2, want: 5 * time.Second},
		{step: 3, want: 30 * time.Second},
		{step: 20, want: 30 * time.Second},
	} {
		if got := recheckSchedulerBackoffDuration(test.step); got != test.want {
			t.Errorf("step=%d backoff=%s want=%s", test.step, got, test.want)
		}
	}
}

type reviewSchedulerTimer struct {
	ch          chan time.Time
	resetEvents chan time.Duration
	resets      []time.Duration
	stopped     int
}

func (t *reviewSchedulerTimer) Chan() <-chan time.Time { return t.ch }

func (t *reviewSchedulerTimer) Stop() bool {
	t.stopped++
	return true
}

func (t *reviewSchedulerTimer) Reset(delay time.Duration) bool {
	t.resets = append(t.resets, delay)
	if t.resetEvents != nil {
		t.resetEvents <- delay
	}
	return true
}

type reviewSchedulerClock struct {
	now   time.Time
	timer *reviewSchedulerTimer
}

type schedulerSQLiteError int

func (e schedulerSQLiteError) Error() string { return "scheduler sqlite error" }

func (e schedulerSQLiteError) SQLiteCode() int { return int(e) }

type schedulerPreAttemptError struct{}

func (schedulerPreAttemptError) Error() string { return "retryable pre-attempt failure" }

func (schedulerPreAttemptError) RecheckPreAttemptDisposition() (string, bool) {
	return "select_job", true
}

func (c *reviewSchedulerClock) Now() time.Time { return c.now }

func (c *reviewSchedulerClock) NewTimer(time.Duration) recheckSchedulerTimer {
	if c.timer == nil {
		c.timer = &reviewSchedulerTimer{ch: make(chan time.Time), resetEvents: make(chan time.Duration, 32)}
	}
	return c.timer
}

func TestRecheckSchedulerProductionRunnerBackoffAndReset(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 1, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	acquisitionError := errors.Join(alertlifecycle.ErrWriterAcquisition, errors.New("temporary writer acquisition"))
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, acquisitionError
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	timer := clock.NewTimer(time.Second)
	scheduler.process(timer, nil)
	if len(clock.timer.resets) != 0 {
		t.Fatal("unclassified acquisition error unexpectedly scheduled a retry")
	}
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5))
	}
	scheduler.process(timer, nil)
	if len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
		t.Fatalf("first acquisition backoff=%v", clock.timer.resets)
	}
	scheduler.process(timer, nil)
	if len(clock.timer.resets) != 2 || clock.timer.resets[1] != 5*time.Second {
		t.Fatalf("second acquisition backoff=%v", clock.timer.resets)
	}
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{JobID: 7, Status: "failed"}, nil
	}
	scheduler.process(timer, nil)
	if len(scheduler.backoffStep) != 0 {
		t.Fatalf("successful writer acquisition did not reset backoff: %v", scheduler.backoffStep)
	}
}

func TestRecheckSchedulerBackoffOwnersAreIndependent(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 2, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	acquisitionError := errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5))
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, acquisitionError
	}
	scheduler.process(timer, nil)
	scheduler.process(timer, nil)

	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckFailurePersistenceError(errors.New("bookkeeping unavailable"), true)
	}
	scheduler.process(timer, nil)

	got := clock.timer.resets
	if len(got) != 3 || got[0] != time.Second || got[1] != 5*time.Second || got[2] != time.Second {
		t.Fatalf("shared backoff state=%v want=[1s 5s 1s]", got)
	}
}

func TestRecheckSchedulerCommitUnknownResetsAcquisitionEdgeBeforeNextFailure(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 2, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5))
	}
	scheduler.process(timer, nil)
	scheduler.process(timer, nil)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckWriterCommitUnknownError(alertlifecycle.RecheckBatchWriterOperation, context.DeadlineExceeded)
	}
	scheduler.process(timer, nil)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5))
	}
	scheduler.process(timer, nil)
	if got, want := clock.timer.resets, []time.Duration{time.Second, 5 * time.Second, time.Second, time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reset sequence=%v want=%v", got, want)
	}
}

func TestRecheckSchedulerInternalDeadlineUsesReconciliationBackoff(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 3, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterCommitOutcomeUnknown, context.DeadlineExceeded)
	}
	scheduler.process(timer, nil)
	if scheduler.ctx.Err() != nil || len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
		t.Fatalf("internal deadline stopped scheduler: ctx=%v resets=%v", scheduler.ctx.Err(), clock.timer.resets)
	}
}

func TestRecheckSchedulerRecoveredBadConnectionUsesAcquisitionBackoff(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 3, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterAcquisition, driver.ErrBadConn)
	}
	scheduler.process(timer, nil)
	if len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
		t.Fatalf("bad-connection backoff=%v", clock.timer.resets)
	}
}

func TestRecheckSchedulerTerminalPreAttemptDoesNotArmTimer(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 3, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterOperationToken, errors.New("entropy unavailable"))
	}
	scheduler.process(timer, nil)
	if len(clock.timer.resets) != 0 {
		t.Fatalf("terminal pre-attempt failure armed timer: %v", clock.timer.resets)
	}
}

func TestRecheckSchedulerLoopUsesIndependentBackoffs(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 4, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	results := []error{
		errors.Join(alertlifecycle.ErrWriterAcquisition, driver.ErrBadConn),
		errors.Join(alertlifecycle.ErrWriterAcquisition, driver.ErrBadConn),
		alertlifecycle.NewRecheckFailurePersistenceError(errors.New("bookkeeping unavailable"), true),
		errors.Join(alertlifecycle.ErrWriterCommitOutcomeUnknown, context.DeadlineExceeded),
		nil,
	}
	calls := make(chan int, len(results))
	call := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		index := call
		call++
		calls <- call
		return alertlifecycle.RecheckBatchResult{}, results[index]
	}
	headRead := make(chan struct{}, 1)
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		headRead <- struct{}{}
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	for index, want := range []time.Duration{time.Second, 5 * time.Second, time.Second, time.Second} {
		if gotCall := <-calls; gotCall != index+1 {
			t.Fatalf("runner call=%d want=%d", gotCall, index+1)
		}
		if got := <-clock.timer.resetEvents; got != want {
			t.Fatalf("backoff %d=%s want=%s", index, got, want)
		}
		clock.timer.ch <- clock.now
	}
	if gotCall := <-calls; gotCall != 5 {
		t.Fatalf("final runner call=%d", gotCall)
	}
	<-headRead
	scheduler.Stop()
}

func TestRecheckSchedulerQueueReadBackoffIsIndependentAndCapped(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 4, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, schedulerSQLiteError(5)
	}
	for _, want := range []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 30 * time.Second} {
		scheduler.process(timer, nil)
		if got := clock.timer.resets[len(clock.timer.resets)-1]; got != want {
			t.Fatalf("queue-read backoff=%s want=%s all=%v", got, want, clock.timer.resets)
		}
	}
}

func TestRecheckSchedulerEachProcessOwnerHasOwnCappedSequence(t *testing.T) {
	for name, processErr := range map[string]error{
		"acquisition":    errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5)),
		"pre-attempt":    schedulerPreAttemptError{},
		"bookkeeping":    alertlifecycle.NewRecheckFailurePersistenceError(errors.New("bookkeeping unavailable"), true),
		"reconciliation": errors.Join(alertlifecycle.ErrWriterCommitOutcomeUnknown, context.DeadlineExceeded),
	} {
		t.Run(name, func(t *testing.T) {
			clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 4, 0, 0, 0, time.UTC)}
			scheduler := newRecheckScheduler(nil, clock)
			timer := clock.NewTimer(time.Second)
			scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{}, processErr
			}
			for _, want := range []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 30 * time.Second} {
				scheduler.process(timer, nil)
				if got := clock.timer.resets[len(clock.timer.resets)-1]; got != want {
					t.Fatalf("%s backoff=%s want=%s all=%v", name, got, want, clock.timer.resets)
				}
			}
		})
	}
}

func TestRecheckSchedulerLoopCoalescesWakeWhileBatchActive(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 4, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	entered := make(chan int, 2)
	releaseFirst := make(chan struct{})
	calls := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		calls++
		entered <- calls
		if calls == 1 {
			<-releaseFirst
		}
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	headReads := make(chan struct{}, 2)
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		headReads <- struct{}{}
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	if first := <-entered; first != 1 {
		t.Fatalf("first call=%d", first)
	}
	scheduler.Signal()
	scheduler.Signal()
	scheduler.Signal()
	close(releaseFirst)
	<-headReads
	if second := <-entered; second != 2 {
		t.Fatalf("second call=%d", second)
	}
	<-headReads
	scheduler.Stop()
	if calls != 2 {
		t.Fatalf("coalesced wake runner calls=%d want=2", calls)
	}
}

func TestRecheckSchedulerLoopDeliversPersistedDueTimer(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 4, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	next := clock.now.Add(30 * time.Second)
	calls := make(chan int, 2)
	call := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		calls <- call
		if call == 1 {
			return alertlifecycle.RecheckBatchResult{}, &alertlifecycleTestRetryScheduledError{next: next}
		}
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	headRead := make(chan struct{}, 1)
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		headRead <- struct{}{}
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	<-calls
	if got := <-clock.timer.resetEvents; got != 30*time.Second {
		t.Fatalf("due timer=%s", got)
	}
	clock.now = next
	clock.timer.ch <- next
	if second := <-calls; second != 2 {
		t.Fatalf("timer runner call=%d", second)
	}
	<-headRead
	scheduler.Stop()
}

func TestRecheckSchedulerConcurrentStartStopHasOneOwner(t *testing.T) {
	scheduler := newRecheckScheduler(nil, nil)
	entered := make(chan struct{}, 1)
	scheduler.runner = func(ctx context.Context, _ int) (alertlifecycle.RecheckBatchResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return alertlifecycle.RecheckBatchResult{}, ctx.Err()
	}
	var starts sync.WaitGroup
	for range 8 {
		starts.Add(1)
		go func() {
			defer starts.Done()
			scheduler.Start()
		}()
	}
	starts.Wait()
	<-entered
	var stops sync.WaitGroup
	for range 8 {
		stops.Add(1)
		go func() {
			defer stops.Done()
			scheduler.Stop()
		}()
	}
	stops.Wait()
	select {
	case <-entered:
		t.Fatal("concurrent Start created a second scheduler owner")
	default:
	}
}

func TestRecheckSchedulerCommittedAbandonRunsQueuedSuccessor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Date(2026, time.August, 19, 5, 0, 0, 0, time.UTC)
	nowRaw := now.Format(time.RFC3339Nano)
	if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at,started_at,finished_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, "review2-blocked-context", "sha256:review2-blocked", `{"formatVersion":1,"sources":[],"clusters":[]}`, "source_context_changed", "failed", 0, 0, nowRaw, nowRaw, nowRaw); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at) VALUES(?,?,?,?,?,?,?,?)`, "review2-successor-context", "sha256:review2-successor", `{"formatVersion":1,"sources":[],"clusters":[]}`, "source_context_changed", "queued", 0, 0, nowRaw); err != nil {
		t.Fatal(err)
	}
	clock := &reviewSchedulerClock{now: now}
	scheduler := newRecheckScheduler(store, clock)
	store.SetRecheckCommitWake(scheduler.Signal)
	productionRunner := scheduler.runner
	type schedulerRun struct {
		result alertlifecycle.RecheckBatchResult
		err    error
	}
	runs := make(chan schedulerRun, 4)
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		promoted, runErr := productionRunner(ctx, batchSize)
		runs <- schedulerRun{result: promoted, err: runErr}
		return promoted, runErr
	}
	scheduler.Start()
	firstRun := <-runs
	if firstRun.err != nil || firstRun.result.Disposition != alertlifecycle.RecheckBatchQueueBlocked {
		t.Fatalf("first scheduler run=%+v", firstRun)
	}
	actor := "review2-operator"
	if _, err = store.AbandonRecheckJobContext(context.Background(), 1, "review2-release", "release head", &actor, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	secondRun := <-runs
	if secondRun.err != nil {
		t.Fatalf("successor scheduler run=%+v", secondRun)
	}
	scheduler.Stop()
	var firstStatus, secondStatus string
	if err = db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&firstStatus); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=2`).Scan(&secondStatus); err != nil {
		t.Fatal(err)
	}
	if firstStatus != "abandoned" || secondStatus != "completed" {
		t.Fatalf("queue release statuses=%s,%s", firstStatus, secondStatus)
	}
}

func TestRecheckSchedulerStartupResumesPersistedQueuedJob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	nowRaw := time.Date(2026, time.August, 19, 5, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if _, err = db.Exec(`INSERT INTO silence_recheck_jobs(source_context_fingerprint,job_fingerprint,source_context_json,trigger_type,status,total_candidates,processed_candidates,created_at) VALUES(?,?,?,?,?,?,?,?)`, "review2-startup-context", "sha256:review2-startup", `{}`, "source_context_changed", "queued", 0, 0, nowRaw); err != nil {
		t.Fatal(err)
	}
	scheduler := newRecheckScheduler(store, &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 5, 0, 0, 0, time.UTC)})
	productionRunner := scheduler.runner
	runDone := make(chan error, 2)
	scheduler.runner = func(ctx context.Context, batchSize int) (alertlifecycle.RecheckBatchResult, error) {
		promoted, runErr := productionRunner(ctx, batchSize)
		runDone <- runErr
		return promoted, runErr
	}
	scheduler.Start()
	if runErr := <-runDone; runErr != nil {
		t.Fatal(runErr)
	}
	scheduler.Stop()
	var status string
	if err = db.QueryRow(`SELECT status FROM silence_recheck_jobs WHERE job_id=1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("startup recovery status=%s", status)
	}
}

func TestCloseAlertLifecycleStoreJoinsSchedulerBeforeStoreClose(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	scheduler := newRecheckScheduler(store, nil)
	entered := make(chan struct{}, 1)
	storeAvailable := make(chan error, 1)
	scheduler.runner = func(ctx context.Context, _ int) (alertlifecycle.RecheckBatchResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		_, statsErr := store.GetStatsContext(context.WithoutCancel(ctx))
		storeAvailable <- statsErr
		return alertlifecycle.RecheckBatchResult{}, ctx.Err()
	}
	previousStore, previousScheduler := lifecycleStore, lifecycleRecheckScheduler
	lifecycleStore, lifecycleRecheckScheduler = store, scheduler
	t.Cleanup(func() {
		lifecycleStore, lifecycleRecheckScheduler = previousStore, previousScheduler
	})
	scheduler.Start()
	<-entered
	if err = closeAlertLifecycleStore(); err != nil {
		t.Fatal(err)
	}
	if statsErr := <-storeAvailable; statsErr != nil {
		t.Fatalf("Store closed before scheduler joined: %v", statsErr)
	}
	if lifecycleStore != nil || lifecycleRecheckScheduler != nil {
		t.Fatal("lifecycle globals were not released")
	}
}

func TestRecheckSchedulerReplacesHeadTimerWithoutSleep(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 1, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, &alertlifecycleTestRetryScheduledError{next: clock.now.Add(5 * time.Second)}
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	timer := clock.NewTimer(time.Second)
	first := scheduler.process(timer, nil)
	if first.timerC == nil || first.terminal || len(clock.timer.resets) != 1 || clock.timer.resets[0] != 5*time.Second {
		t.Fatalf("first head timer=%v resets=%v", first, clock.timer.resets)
	}
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, nil
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		next := clock.now.Add(time.Second)
		return alertlifecycle.RecheckQueueHead{JobID: 7, Status: "retry_wait", NextAttemptAt: &next}, nil
	}
	second := scheduler.process(timer, first.timerC)
	if second.timerC == nil || second.terminal || len(clock.timer.resets) != 2 || clock.timer.resets[1] != time.Second {
		t.Fatalf("replacement head timer=%v resets=%v", second, clock.timer.resets)
	}
}

func TestRecheckSchedulerStartupAndCancellationOwnOneRunner(t *testing.T) {
	scheduler := newRecheckScheduler(nil, nil)
	started := make(chan struct{})
	finished := make(chan struct{})
	scheduler.runner = func(ctx context.Context, _ int) (alertlifecycle.RecheckBatchResult, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return alertlifecycle.RecheckBatchResult{}, ctx.Err()
	}
	scheduler.head = func(context.Context) (alertlifecycle.RecheckQueueHead, error) {
		return alertlifecycle.RecheckQueueHead{}, nil
	}
	scheduler.Start()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not run its startup recovery")
	}
	scheduler.Stop()
	select {
	case <-finished:
	default:
		t.Fatal("scheduler stopped before its active runner returned")
	}
}

type alertlifecycleTestRetryScheduledError struct{ next time.Time }

func (e *alertlifecycleTestRetryScheduledError) Error() string { return "retry scheduled" }

func (e *alertlifecycleTestRetryScheduledError) NextTime() time.Time { return e.next }

package alertlifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegroupReconciliationUsesDeterministicOneSecondTimer(t *testing.T) {
	original := regroupReconciliationTimer
	t.Cleanup(func() { regroupReconciliationTimer = original })
	timerRequested := make(chan time.Duration, 1)
	timerFired := make(chan time.Time)
	regroupReconciliationTimer = func(duration time.Duration) (<-chan time.Time, func()) {
		timerRequested <- duration
		return timerFired, func() {}
	}
	completion := newRegroupPhaseCompletion(nil)
	result := make(chan struct {
		committed bool
		err       error
	}, 1)
	go func() {
		committed, err := waitRegroupCommitted(context.Background(), completion)
		result <- struct {
			committed bool
			err       error
		}{committed: committed, err: err}
	}()
	select {
	case duration := <-timerRequested:
		if duration != time.Second {
			t.Fatalf("reconciliation timer: got=%s want=%s", duration, time.Second)
		}
	case <-time.After(time.Second):
		t.Fatal("reconciliation timer was not requested")
	}
	timerFired <- time.Now()
	select {
	case outcome := <-result:
		if outcome.committed || outcome.err != nil {
			t.Fatalf("timer outcome: committed=%t err=%v", outcome.committed, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconciliation wait did not finish after timer release")
	}
}

func TestRegroupReconciliationTimerWaitHonorsCancellation(t *testing.T) {
	original := regroupReconciliationTimer
	t.Cleanup(func() { regroupReconciliationTimer = original })
	timerRequested := make(chan time.Duration, 1)
	regroupReconciliationTimer = func(duration time.Duration) (<-chan time.Time, func()) {
		timerRequested <- duration
		return make(chan time.Time), func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := waitRegroupFailureCommitted(ctx, newRecheckFailureCompletion())
		result <- err
	}()
	select {
	case <-timerRequested:
	case <-time.After(time.Second):
		t.Fatal("failure reconciliation timer was not requested")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failure reconciliation wait ignored cancellation")
	}
}

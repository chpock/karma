package main

import (
	"errors"
	"testing"
	"time"
)

func TestHistoryPollerStopJoinsWorkersWithoutClosingQueue(t *testing.T) {
	hp := newHistoryPoller(1, time.Second)
	go hp.run(1)
	select {
	case err := <-hp.startResult:
		if err != nil {
			t.Fatalf("history poller startup failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("history poller did not start")
	}
	hp.stop()
	if hp.isRunning.Load() {
		t.Fatal("history poller still reports running after stop")
	}
	// Stop is idempotent and submit must not send to a closed queue.
	hp.stop()
	if err := hp.submit("http://localhost", map[string]string{"alertname": "test"}, make(chan historyQueryResult, 1)); !errors.Is(err, errHistoryPollerStopped) {
		t.Fatalf("submit after stop: got=%v want=%v", err, errHistoryPollerStopped)
	}
}

func TestHistoryPollerSubmitReportsStoppedState(t *testing.T) {
	hp := newHistoryPoller(1, time.Second)
	result := make(chan historyQueryResult, 1)
	if err := hp.submit("http://localhost", map[string]string{"alertname": "test"}, result); !errors.Is(err, errHistoryPollerStopped) {
		t.Fatalf("submit before start: got=%v want=%v", err, errHistoryPollerStopped)
	}
	hp.stopQuiet()
	if err := hp.submit("http://localhost", map[string]string{"alertname": "test"}, result); !errors.Is(err, errHistoryPollerStopped) {
		t.Fatalf("submit after stop: got=%v want=%v", err, errHistoryPollerStopped)
	}
}

func TestHistoryPollerStartupReportsStopWinner(t *testing.T) {
	hp := newHistoryPoller(1, time.Second)
	hp.stop()
	go hp.run(1)
	select {
	case err := <-hp.startResult:
		if !errors.Is(err, errHistoryPollerStopped) {
			t.Fatalf("startup result: got=%v want=%v", err, errHistoryPollerStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("history poller did not report startup result")
	}
	select {
	case <-hp.done:
	case <-time.After(time.Second):
		t.Fatal("history poller did not finish after startup stop")
	}
}

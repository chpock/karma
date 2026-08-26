package alertlifecycle

import (
	"testing"
	"time"
)

func runCompleteRecheckStateEventMatrix(t *testing.T) {
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	states := []string{"queued", "running", "retry_wait", "failed", "completed", "abandoned"}
	events := []recheckJobEvent{recheckJobSelected, recheckJobBatchCommitted, recheckJobBatchCompleted, recheckJobRetryableFailure, recheckJobTerminalFailure, recheckJobOperatorRetry, recheckJobOperatorAbandon}
	type cell struct {
		state   string
		event   recheckJobEvent
		want    recheckJobState
		wantErr bool
	}
	queued := recheckJobState{Status: "queued", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(42 * time.Second)), FinishedAt: timePtr(now.Add(-2 * time.Minute)), AbandonedAt: timePtr(now.Add(-3 * time.Minute))}
	running := recheckJobState{Status: "running", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(42 * time.Second)), FinishedAt: timePtr(now.Add(-2 * time.Minute)), AbandonedAt: timePtr(now.Add(-3 * time.Minute))}
	retryWait := recheckJobState{Status: "retry_wait", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(42 * time.Second)), FinishedAt: timePtr(now.Add(-2 * time.Minute)), AbandonedAt: timePtr(now.Add(-3 * time.Minute))}
	failed := recheckJobState{Status: "failed", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(42 * time.Second)), FinishedAt: timePtr(now.Add(-2 * time.Minute)), AbandonedAt: timePtr(now.Add(-3 * time.Minute))}
	completed := recheckJobState{Status: "completed", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(42 * time.Second)), FinishedAt: timePtr(now.Add(-2 * time.Minute)), AbandonedAt: timePtr(now.Add(-3 * time.Minute))}
	abandoned := recheckJobState{Status: "abandoned", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(42 * time.Second)), FinishedAt: timePtr(now.Add(-2 * time.Minute)), AbandonedAt: timePtr(now.Add(-3 * time.Minute))}
	cells := []cell{
		{state: "queued", event: recheckJobSelected, want: recheckJobState{Status: "running", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, FinishedAt: queued.FinishedAt, AbandonedAt: queued.AbandonedAt}},
		{state: "queued", event: recheckJobBatchCommitted, wantErr: true},
		{state: "queued", event: recheckJobBatchCompleted, wantErr: true},
		{state: "queued", event: recheckJobRetryableFailure, wantErr: true},
		{state: "queued", event: recheckJobTerminalFailure, wantErr: true},
		{state: "queued", event: recheckJobOperatorRetry, wantErr: true},
		{state: "queued", event: recheckJobOperatorAbandon, wantErr: true},
		{state: "running", event: recheckJobSelected, want: recheckJobState{Status: "running", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, FinishedAt: running.FinishedAt, AbandonedAt: running.AbandonedAt}},
		{state: "running", event: recheckJobBatchCommitted, want: recheckJobState{Status: "running", RetryCount: 7, RecoveryCount: 4, FinishedAt: running.FinishedAt, AbandonedAt: running.AbandonedAt}},
		{state: "running", event: recheckJobBatchCompleted, want: recheckJobState{Status: "completed", RetryCount: 7, RecoveryCount: 4, FinishedAt: &now, AbandonedAt: running.AbandonedAt}},
		{state: "running", event: recheckJobRetryableFailure, want: recheckJobState{Status: "retry_wait", RetryCount: 8, ConsecutiveRetryCount: 4, RecoveryCount: 4, NextAttemptAt: timePtr(now.Add(2 * time.Minute)), FinishedAt: running.FinishedAt, AbandonedAt: running.AbandonedAt}},
		{state: "running", event: recheckJobTerminalFailure, want: recheckJobState{Status: "failed", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, FinishedAt: &now, AbandonedAt: running.AbandonedAt}},
		{state: "running", event: recheckJobOperatorRetry, wantErr: true},
		{state: "running", event: recheckJobOperatorAbandon, wantErr: true},
		{state: "retry_wait", event: recheckJobSelected, want: recheckJobState{Status: "running", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, FinishedAt: retryWait.FinishedAt, AbandonedAt: retryWait.AbandonedAt}},
		{state: "retry_wait", event: recheckJobBatchCommitted, wantErr: true},
		{state: "retry_wait", event: recheckJobBatchCompleted, wantErr: true},
		{state: "retry_wait", event: recheckJobRetryableFailure, wantErr: true},
		{state: "retry_wait", event: recheckJobTerminalFailure, wantErr: true},
		{state: "retry_wait", event: recheckJobOperatorRetry, wantErr: true},
		{state: "retry_wait", event: recheckJobOperatorAbandon, wantErr: true},
		{state: "failed", event: recheckJobSelected, wantErr: true},
		{state: "failed", event: recheckJobBatchCommitted, wantErr: true},
		{state: "failed", event: recheckJobBatchCompleted, wantErr: true},
		{state: "failed", event: recheckJobRetryableFailure, wantErr: true},
		{state: "failed", event: recheckJobTerminalFailure, wantErr: true},
		{state: "failed", event: recheckJobOperatorRetry, want: recheckJobState{Status: "queued", RetryCount: 7, RecoveryCount: 5}},
		{state: "failed", event: recheckJobOperatorAbandon, want: recheckJobState{Status: "abandoned", RetryCount: 7, ConsecutiveRetryCount: 3, RecoveryCount: 4, FinishedAt: &now, AbandonedAt: &now}},
		{state: "completed", event: recheckJobSelected, wantErr: true},
		{state: "completed", event: recheckJobBatchCommitted, wantErr: true},
		{state: "completed", event: recheckJobBatchCompleted, wantErr: true},
		{state: "completed", event: recheckJobRetryableFailure, wantErr: true},
		{state: "completed", event: recheckJobTerminalFailure, wantErr: true},
		{state: "completed", event: recheckJobOperatorRetry, wantErr: true},
		{state: "completed", event: recheckJobOperatorAbandon, wantErr: true},
		{state: "abandoned", event: recheckJobSelected, wantErr: true},
		{state: "abandoned", event: recheckJobBatchCommitted, wantErr: true},
		{state: "abandoned", event: recheckJobBatchCompleted, wantErr: true},
		{state: "abandoned", event: recheckJobRetryableFailure, wantErr: true},
		{state: "abandoned", event: recheckJobTerminalFailure, wantErr: true},
		{state: "abandoned", event: recheckJobOperatorRetry, wantErr: true},
		{state: "abandoned", event: recheckJobOperatorAbandon, wantErr: true},
	}
	if len(cells) != len(states)*len(events) {
		t.Fatalf("matrix cells=%d want=%d", len(cells), len(states)*len(events))
	}
	seen := make(map[string]bool, len(cells))
	for _, test := range cells {
		key := test.state + "/" + string(test.event)
		if seen[key] {
			t.Fatalf("duplicate matrix cell %s", key)
		}
		seen[key] = true
		initial := map[string]recheckJobState{"queued": queued, "running": running, "retry_wait": retryWait, "failed": failed, "completed": completed, "abandoned": abandoned}[test.state]
		got, err := applyRecheckJobEvent(initial, test.event, now)
		if test.wantErr {
			if err == nil || !sameRecheckJobState(got, initial) {
				t.Fatalf("rejected %s got=%+v err=%v initial=%+v", key, got, err, initial)
			}
			continue
		}
		if err != nil || !sameRecheckJobState(got, test.want) {
			t.Fatalf("cell %s got=%+v err=%v want=%+v", key, got, err, test.want)
		}
	}
	if len(seen) != len(states)*len(events) {
		t.Fatalf("matrix unique cells=%d want=%d", len(seen), len(states)*len(events))
	}
}

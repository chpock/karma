package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem3Review8SchedulerRawUnavailableFailsClosed(t *testing.T) {
	for name, cause := range map[string]error{
		"raw_unavailable":     errors.Join(alertlifecycle.ErrWriterUnavailable, errors.New("review8 raw unavailable")),
		"replacement_full":    errors.Join(alertlifecycle.ErrWriterUnavailable, schedulerSQLiteError(13)),
		"replacement_corrupt": errors.Join(alertlifecycle.ErrWriterUnavailable, schedulerSQLiteError(11)),
		"replacement_unknown": errors.Join(alertlifecycle.ErrWriterUnavailable, errors.New("review8 unknown replacement")),
	} {
		t.Run(name, func(t *testing.T) {
			clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 7, 0, 0, 0, time.UTC)}
			scheduler := newRecheckScheduler(nil, clock)
			timer := clock.NewTimer(time.Second)
			scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{}, cause
			}

			result := scheduler.process(timer, nil)
			if !result.terminal || len(clock.timer.resets) != 0 {
				t.Fatalf("raw unavailable disposition result=%+v resets=%v", result, clock.timer.resets)
			}
		})
	}
}

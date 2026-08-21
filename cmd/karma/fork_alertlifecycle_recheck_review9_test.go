package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem3Review9SchedulerTypedUnavailableTerminalCausesStopWithoutTimer(t *testing.T) {
	for name, cause := range map[string]error{
		"full":    errors.Join(alertlifecycle.ErrWriterAcquisition, alertlifecycle.ErrWriterUnavailable, schedulerSQLiteError(13)),
		"corrupt": errors.Join(alertlifecycle.ErrWriterAcquisition, alertlifecycle.ErrWriterUnavailable, schedulerSQLiteError(11)),
		"unknown": errors.Join(alertlifecycle.ErrWriterAcquisition, alertlifecycle.ErrWriterUnavailable, errors.New("review9 unknown replacement cause")),
	} {
		t.Run(name, func(t *testing.T) {
			clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)}
			scheduler := newRecheckScheduler(nil, clock)
			timer := clock.NewTimer(time.Second)
			scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
				return alertlifecycle.RecheckBatchResult{}, alertlifecycle.NewRecheckFailurePersistenceError(cause, false)
			}

			result := scheduler.process(timer, nil)
			if !result.terminal || len(clock.timer.resets) != 0 {
				t.Fatalf("typed unavailable terminal disposition result=%+v resets=%v", result, clock.timer.resets)
			}
		})
	}
}

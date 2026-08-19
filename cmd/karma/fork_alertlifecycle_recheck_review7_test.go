package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem3Review7SchedulerAcquisitionPrecedenceMatrix(t *testing.T) {
	for name, test := range map[string]struct {
		cause        error
		wantTimer    bool
		wantTerminal bool
	}{
		"bad_connection":            {cause: errors.Join(alertlifecycle.ErrWriterAcquisition, driver.ErrBadConn), wantTimer: true},
		"connection_done":           {cause: errors.Join(alertlifecycle.ErrWriterAcquisition, sql.ErrConnDone), wantTimer: true},
		"sqlite_busy":               {cause: errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(5)), wantTimer: true},
		"sqlite_full":               {cause: errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(13)), wantTerminal: true},
		"sqlite_corrupt":            {cause: errors.Join(alertlifecycle.ErrWriterAcquisition, schedulerSQLiteError(11)), wantTerminal: true},
		"unknown":                   {cause: errors.Join(alertlifecycle.ErrWriterAcquisition, errors.New("review7 unknown acquisition")), wantTerminal: true},
		"terminal_callback_cleanup": {cause: errors.Join(alertlifecycle.ErrWriterOperationInvariant, alertlifecycle.ErrWriterUnavailable), wantTerminal: true},
	} {
		t.Run(name, func(t *testing.T) {
			clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 6, 0, 0, 0, time.UTC)}
			scheduler := newRecheckScheduler(nil, clock)
			timer := clock.NewTimer(time.Second)
			scheduler.runner = func(context.Context, int) (int, error) {
				return 0, alertlifecycle.NewRecheckFailurePersistenceError(test.cause, test.wantTimer)
			}

			result := scheduler.process(timer, nil)
			if test.wantTimer {
				if result.terminal || len(clock.timer.resets) != 1 || clock.timer.resets[0] != time.Second {
					t.Fatalf("acquisition disposition result=%+v resets=%v", result, clock.timer.resets)
				}
				return
			}
			if !test.wantTerminal || !result.terminal || len(clock.timer.resets) != 0 {
				t.Fatalf("terminal disposition result=%+v resets=%v", result, clock.timer.resets)
			}
		})
	}
}

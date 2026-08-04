package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prymitive/karma/internal/config"
)

var lifecycleHousekeeping struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func startAlertLifecycleHousekeeping() {
	if lifecycleStore == nil || !config.Config.AlertLifecycle.Enabled {
		return
	}

	interval := config.Config.AlertLifecycle.Housekeeping.IncrementalVacuumInterval
	ctx, cancel := context.WithCancel(context.Background())

	lifecycleHousekeeping.mu.Lock()
	lifecycleHousekeeping.cancel = cancel
	lifecycleHousekeeping.done = make(chan struct{})
	done := lifecycleHousekeeping.done
	lifecycleHousekeeping.mu.Unlock()

	go func() {
		defer close(done)

		tick := time.NewTicker(interval)
		defer tick.Stop()

		runAlertLifecycleHousekeepingOnce()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				runAlertLifecycleHousekeepingOnce()
			}
		}
	}()
}

func stopAlertLifecycleHousekeeping() {
	lifecycleHousekeeping.mu.Lock()
	cancel := lifecycleHousekeeping.cancel
	done := lifecycleHousekeeping.done
	lifecycleHousekeeping.cancel = nil
	lifecycleHousekeeping.done = nil
	lifecycleHousekeeping.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func runAlertLifecycleHousekeepingOnce() {
	if lifecycleStore == nil {
		return
	}

	result, err := lifecycleStore.RunHousekeeping(
		config.Config.AlertLifecycle.Retention.TTL,
		config.Config.AlertLifecycle.Retention.MaxAlerts,
	)
	if err != nil {
		slog.Error("Alert lifecycle housekeeping failed", slog.Any("error", err))
		return
	}

	slog.Info(
		"Alert lifecycle housekeeping completed",
		slog.Int("expiredDeleted", result.ExpiredDeleted),
		slog.Int("limitDeleted", result.LimitDeleted),
		slog.Int("vacuumPagesFreed", result.VacuumPagesFreed),
		slog.Int("currentRowsAfter", result.CurrentRowsAfter),
		slog.Int("historyRowsAfter", result.HistoryRowsAfter),
	)
}

package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prymitive/karma/internal/config"
	runtimeLog "github.com/prymitive/karma/internal/log"
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

		runAlertLifecycleHousekeepingOnce(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				runAlertLifecycleHousekeepingOnce(ctx)
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

func runAlertLifecycleHousekeepingOnce(ctx context.Context) {
	if lifecycleStore == nil {
		return
	}

	startedAt := time.Now()
	runtimeLog.Event(ctx, slog.LevelDebug, "lifecycle_housekeeping", "housekeeping_started", "Alert lifecycle housekeeping started", slog.Duration("retention_ttl", config.Config.AlertLifecycle.Retention.TTL), slog.Duration("silence_history_retention_ttl", config.Config.AlertLifecycle.SilenceHistory.Retention.TTL), slog.Int("max_alerts", config.Config.AlertLifecycle.Retention.MaxAlerts))
	result, err := lifecycleStore.RunHousekeepingContext(
		ctx,
		config.Config.AlertLifecycle.Retention.TTL,
		config.Config.AlertLifecycle.Retention.MaxAlerts,
	)
	if err != nil {
		runtimeLog.Event(ctx, slog.LevelError, "lifecycle_housekeeping", "housekeeping_failed", "Alert lifecycle housekeeping failed", slog.String("stage", "execute"), slog.Any("error", err), slog.Duration("duration", time.Since(startedAt)))
		return
	}

	level := slog.LevelDebug
	if result.ExpiredDeleted+result.LimitDeleted+result.RecheckJobsDeleted+result.RetiredSourcesDeleted+result.RevisionsDeleted+result.EpisodesDeleted+result.RulesDeleted+result.VacuumPagesFreed > 0 {
		level = slog.LevelInfo
	}
	runtimeLog.Event(ctx, level,
		"lifecycle_housekeeping", "housekeeping_completed", "Alert lifecycle housekeeping completed",
		slog.Int("expiredDeleted", result.ExpiredDeleted),
		slog.Int("limitDeleted", result.LimitDeleted),
		slog.Int("recheckJobsDeleted", result.RecheckJobsDeleted),
		slog.Int("retiredSourcesDeleted", result.RetiredSourcesDeleted),
		slog.Int("revisionsDeleted", result.RevisionsDeleted),
		slog.Int("episodesDeleted", result.EpisodesDeleted),
		slog.Int("rulesDeleted", result.RulesDeleted),
		slog.Int("vacuumPagesFreed", result.VacuumPagesFreed),
		slog.Int("currentRowsAfter", result.CurrentRowsAfter),
		slog.Int("historyRowsAfter", result.HistoryRowsAfter),
		slog.Duration("duration", time.Since(startedAt)),
	)

	runAlertLifecycleRecheckOnce(ctx)
}

func runAlertLifecycleRecheckOnce(ctx context.Context) {
	if lifecycleStore == nil {
		return
	}
	promoted, err := lifecycleStore.RunSilenceRecheckBatchContext(ctx, 1000)
	if err != nil {
		runtimeLog.Event(ctx, slog.LevelError, "silence_recheck", "worker_failed", "Alert lifecycle silence recheck failed", slog.Any("error", err))
		return
	}
	if promoted > 0 {
		runtimeLog.Event(ctx, slog.LevelInfo, "silence_recheck", "batch_promoted", "Alert lifecycle silence recheck promoted alerts", slog.Int("promoted", promoted))
	}
}

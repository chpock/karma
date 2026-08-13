package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
	runtimeLog "github.com/prymitive/karma/internal/log"
)

var lifecycleStore *alertlifecycle.Store

var lifecycleRegroup struct {
	cancel context.CancelFunc
}

func setupAlertLifecycleStorage() error {
	if !config.Config.AlertLifecycle.Enabled {
		runtimeLog.Debug("alert_lifecycle", "storage_disabled", "Alert lifecycle storage is disabled")
		return nil
	}

	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{
		Path:                       config.Config.AlertLifecycle.SQLite.Path,
		SilenceHistoryRetentionTTL: config.Config.AlertLifecycle.SilenceHistory.Retention.TTL,
	})
	if err != nil {
		runtimeLog.Error("alert_lifecycle", "storage_open_failed", "Failed to initialize alert lifecycle storage", slog.String("path", config.Config.AlertLifecycle.SQLite.Path), slog.Any("error", err))
		return fmt.Errorf("failed to initialize alert lifecycle storage: %w", err)
	}

	stats, statsErr := store.GetStats()
	currentRows := -1
	if statsErr != nil {
		runtimeLog.Error("alert_lifecycle", "storage_stats_failed", "Failed to read initialized alert lifecycle storage stats", slog.String("path", config.Config.AlertLifecycle.SQLite.Path), slog.Any("error", statsErr))
	} else {
		currentRows = stats.CurrentRows
	}
	lifecycleStore = store
	runtimeLog.Info("alert_lifecycle", "storage_initialized", "Alert lifecycle storage initialized", slog.String("path", config.Config.AlertLifecycle.SQLite.Path), slog.String("schema_version", "1"), slog.Int("current_rows", currentRows))
	startAlertLifecycleBackgroundRegroup()
	startAlertLifecycleHousekeeping()
	runtimeLog.Debug("alert_lifecycle", "background_workers_started", "Alert lifecycle background workers started", slog.Duration("housekeeping_interval", config.Config.AlertLifecycle.Housekeeping.IncrementalVacuumInterval), slog.Int("regroup_batch_size", 1000), slog.Int("recheck_batch_size", 1000))

	return nil
}

func closeAlertLifecycleStorage() {
	stopAlertLifecycleBackgroundRegroup()
	stopAlertLifecycleHousekeeping()

	if lifecycleStore == nil {
		return
	}
	if err := lifecycleStore.Close(); err != nil {
		runtimeLog.Error("alert_lifecycle", "storage_close_failed", "Failed to close alert lifecycle storage", slog.Any("error", err))
	}
	lifecycleStore = nil
	runtimeLog.Debug("alert_lifecycle", "background_workers_stopped", "Alert lifecycle background workers stopped")
}

func startAlertLifecycleBackgroundRegroup() {
	if lifecycleStore == nil {
		return
	}

	cfg := alertlifecycle.GroupingConfig{
		LabelsKeep:       append([]string{}, config.Config.Labels.Keep...),
		LabelsKeepRegex:  append([]string{}, config.Config.Labels.KeepRegex...),
		LabelsStrip:      append([]string{}, config.Config.Labels.Strip...),
		LabelsStripRegex: append([]string{}, config.Config.Labels.StripRegex...),
	}

	ctx, cancel := context.WithCancel(context.Background())
	lifecycleRegroup.cancel = cancel

	go func() {
		startedAt := time.Now()
		result, err := lifecycleStore.RunRegroupToConfig(ctx, cfg, 1000)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				runtimeLog.Event(ctx, slog.LevelDebug, "lifecycle_regroup", "regroup_cancelled", "Alert lifecycle regroup cancelled", slog.Duration("duration", time.Since(startedAt)))
				return
			}
			runtimeLog.Event(ctx, slog.LevelError, "lifecycle_regroup", "regroup_failed", "Alert lifecycle background regroup failed", slog.Any("error", err), slog.Duration("duration", time.Since(startedAt)))
			return
		}
		if result.Processed == 0 && result.Updated == 0 {
			runtimeLog.Event(ctx, slog.LevelDebug, "lifecycle_regroup", "regroup_not_required", "Alert lifecycle regroup is not required", slog.Int("total", result.Total), slog.Duration("duration", time.Since(startedAt)))
			return
		}
		runtimeLog.Event(ctx, slog.LevelInfo,
			"lifecycle_regroup", "regroup_completed", "Alert lifecycle background regroup completed",
			slog.Int("processed", result.Processed),
			slog.Int("updated", result.Updated),
			slog.Int("total", result.Total),
			slog.Duration("duration", time.Since(startedAt)),
		)
	}()
}

func stopAlertLifecycleBackgroundRegroup() {
	if lifecycleRegroup.cancel != nil {
		lifecycleRegroup.cancel()
		lifecycleRegroup.cancel = nil
	}
}

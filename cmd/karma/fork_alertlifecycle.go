package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

var lifecycleStore *alertlifecycle.Store

var lifecycleRegroup struct {
	cancel context.CancelFunc
}

func setupAlertLifecycleStorage() error {
	if !config.Config.AlertLifecycle.Enabled {
		slog.Info("Alert lifecycle storage is disabled")
		return nil
	}

	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{
		Path: config.Config.AlertLifecycle.SQLite.Path,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize alert lifecycle storage: %w", err)
	}

	lifecycleStore = store
	slog.Info("Alert lifecycle storage initialized", slog.String("path", config.Config.AlertLifecycle.SQLite.Path))
	startAlertLifecycleBackgroundRegroup()
	startAlertLifecycleHousekeeping()

	return nil
}

func closeAlertLifecycleStorage() {
	stopAlertLifecycleBackgroundRegroup()
	stopAlertLifecycleHousekeeping()

	if lifecycleStore == nil {
		return
	}
	if err := lifecycleStore.Close(); err != nil {
		slog.Error("Failed to close alert lifecycle storage", slog.Any("error", err))
	}
	lifecycleStore = nil
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
		result, err := lifecycleStore.RunRegroupToConfig(ctx, cfg, 1000)
		if err != nil {
			slog.Error("Alert lifecycle background regroup failed", slog.Any("error", err))
			return
		}
		slog.Info(
			"Alert lifecycle background regroup completed",
			slog.Int("processed", result.Processed),
			slog.Int("updated", result.Updated),
			slog.Int("total", result.Total),
		)
	}()
}

func stopAlertLifecycleBackgroundRegroup() {
	if lifecycleRegroup.cancel != nil {
		lifecycleRegroup.cancel()
		lifecycleRegroup.cancel = nil
	}
}

package main

import (
	"fmt"
	"log/slog"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

var lifecycleStore *alertlifecycle.Store

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
	startAlertLifecycleHousekeeping()

	return nil
}

func closeAlertLifecycleStorage() {
	stopAlertLifecycleHousekeeping()

	if lifecycleStore == nil {
		return
	}
	if err := lifecycleStore.Close(); err != nil {
		slog.Error("Failed to close alert lifecycle storage", slog.Any("error", err))
	}
	lifecycleStore = nil
}

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

var lifecycleRegroup regroupOwner

var lifecycleRecheckScheduler *recheckScheduler

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
	lifecycleRecheckScheduler = newRecheckScheduler(store, nil)
	store.SetRecheckCommitWake(signalAlertLifecycleRecheck)
	store.SetRecheckReconciledWake(signalAlertLifecycleRecheckReconciled)
	lifecycleRecheckScheduler.Start()
	runtimeLog.Info("alert_lifecycle", "storage_initialized", "Alert lifecycle storage initialized", slog.String("path", config.Config.AlertLifecycle.SQLite.Path), slog.String("schema_version", "1"), slog.Int("current_rows", currentRows))
	startAlertLifecycleBackgroundRegroup()
	startAlertLifecycleHousekeeping()
	runtimeLog.Debug("alert_lifecycle", "background_workers_started", "Alert lifecycle background workers started", slog.Duration("housekeeping_interval", config.Config.AlertLifecycle.Housekeeping.IncrementalVacuumInterval), slog.Int("regroup_batch_size", 1000), slog.Int("recheck_batch_size", 1000))

	return nil
}

func closeAlertLifecycleStore() error {
	if lifecycleRecheckScheduler != nil {
		lifecycleRecheckScheduler.Stop()
		lifecycleRecheckScheduler = nil
	}
	if lifecycleStore == nil {
		return nil
	}
	err := lifecycleStore.Close()
	if err != nil {
		runtimeLog.Error("alert_lifecycle", "storage_close_failed", "Failed to close alert lifecycle storage", slog.Any("error", err))
	}
	lifecycleStore = nil
	runtimeLog.Debug("alert_lifecycle", "background_workers_stopped", "Alert lifecycle background workers stopped")
	return err
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

	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	if !lifecycleRegroup.start(done, regroupOwnerDependencies(cancel, done)) {
		cancel(alertlifecycle.ErrRegroupExpectedShutdown)
		return
	}

	go func() {
		defer close(done)
		startedAt := time.Now()
		result, err := lifecycleStore.RunRegroupToConfig(ctx, cfg, 1000)
		logAlertLifecycleRegroupResult(ctx, cfg, startedAt, result, err)
	}()
}

func logAlertLifecycleRegroupResult(ctx context.Context, cfg alertlifecycle.GroupingConfig, startedAt time.Time, result alertlifecycle.RegroupResult, err error) {
	if err != nil {
		var infrastructureErr *alertlifecycle.RegroupInfrastructureError
		if errors.As(err, &infrastructureErr) {
			if !infrastructureErr.Valid() {
				runtimeLog.ErrorContext(ctx, "lifecycle_regroup", "regroup_infrastructure_contract_violation", "Invalid regroup infrastructure contract", slog.String("stage", string(infrastructureErr.Stage)), slog.String("reason_code", string(infrastructureErr.ReasonCode)), slog.Any("error", errors.Join(alertlifecycle.ErrRegroupInfrastructureContract, err)))
				return
			}
			if isDirectRegroupCancellation(infrastructureErr.Stage, infrastructureErr.ReasonCode) {
				level := regroupInfrastructureLogLevel(infrastructureErr.Stage, infrastructureErr.ReasonCode, infrastructureErr.ExpectedShutdown)
				runtimeLog.Event(ctx, level, "lifecycle_regroup", "regroup_cancelled", "Alert lifecycle regroup cancelled", slog.String("reason_code", string(infrastructureErr.ReasonCode)), slog.Bool("expected_shutdown", infrastructureErr.ExpectedShutdown), slog.Duration("duration", time.Since(startedAt)))
				return
			}
			level := regroupInfrastructureLogLevel(infrastructureErr.Stage, infrastructureErr.ReasonCode, infrastructureErr.ExpectedShutdown)
			runtimeLog.Event(ctx, level, "lifecycle_regroup", "regroup_infrastructure_blocked", "Alert lifecycle regroup infrastructure blocked progress", slog.String("target_fingerprint", alertlifecycle.BuildGroupingFingerprint(cfg)), slog.String("stage", string(infrastructureErr.Stage)), slog.String("reason_code", string(infrastructureErr.ReasonCode)), slog.Int("processed", infrastructureErr.Processed), slog.Int("total", infrastructureErr.Total), slog.Any("error", infrastructureErr), slog.Duration("duration", time.Since(startedAt)))
			return
		}
		if errors.Is(err, alertlifecycle.ErrRegroupFailurePersisted) {
			return
		}
		if errors.Is(err, context.Canceled) {
			runtimeLog.Event(ctx, slog.LevelWarn, "lifecycle_regroup", "regroup_cancelled", "Alert lifecycle regroup cancelled", slog.Bool("expected_shutdown", false), slog.Duration("duration", time.Since(startedAt)))
			return
		}
		runtimeLog.Event(ctx, slog.LevelError, "lifecycle_regroup", "regroup_infrastructure_blocked", "Alert lifecycle regroup returned an unclassified error", slog.String("target_fingerprint", alertlifecycle.BuildGroupingFingerprint(cfg)), slog.String("stage", "unknown"), slog.String("reason_code", string(alertlifecycle.RegroupReasonUnclassifiedError)), slog.Int("processed", result.Processed), slog.Int("total", result.Total), slog.Any("error", err), slog.Duration("duration", time.Since(startedAt)))
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
}

func stopAlertLifecycleBackgroundRegroup() {
	lifecycleRegroup.stop(alertlifecycle.ErrRegroupExpectedShutdown)
}

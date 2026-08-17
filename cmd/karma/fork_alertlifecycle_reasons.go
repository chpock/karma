package main

import (
	"log/slog"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func isDirectRegroupCancellation(stage alertlifecycle.RegroupInfrastructureStage, reason alertlifecycle.RegroupInfrastructureReason) bool {
	return (stage == alertlifecycle.RegroupStageBegin || stage == alertlifecycle.RegroupStageBatch) &&
		(reason == alertlifecycle.RegroupReasonCancelled || reason == alertlifecycle.RegroupReasonDeadline)
}

func regroupInfrastructureLogLevel(stage alertlifecycle.RegroupInfrastructureStage, reason alertlifecycle.RegroupInfrastructureReason, expectedShutdown bool) slog.Level {
	if isDirectRegroupCancellation(stage, reason) {
		if expectedShutdown {
			return slog.LevelDebug
		}
		return slog.LevelWarn
	}
	switch string(reason) {
	case string(alertlifecycle.RegroupReasonStoreClosing),
		string(alertlifecycle.RegroupReasonStoreClosed),
		string(alertlifecycle.RegroupReasonFailurePersistenceCancelled),
		string(alertlifecycle.RegroupReasonFailurePersistenceDeadline),
		string(alertlifecycle.RegroupReasonFailurePersistenceStoreClosing),
		string(alertlifecycle.RegroupReasonFailurePersistenceStoreClosed):
		if expectedShutdown {
			return slog.LevelDebug
		}
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

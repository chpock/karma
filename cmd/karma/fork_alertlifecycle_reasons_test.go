package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestRegroupInfrastructureReasonSeverityUsesExactPairs(t *testing.T) {
	tests := []struct {
		name     string
		stage    alertlifecycle.RegroupInfrastructureStage
		reason   alertlifecycle.RegroupInfrastructureReason
		expected bool
		want     slog.Level
	}{
		{name: "expected direct cancellation", stage: alertlifecycle.RegroupStageBatch, reason: alertlifecycle.RegroupReasonCancelled, expected: true, want: slog.LevelDebug},
		{name: "external direct cancellation", stage: alertlifecycle.RegroupStageBatch, reason: alertlifecycle.RegroupReasonCancelled, want: slog.LevelWarn},
		{name: "expected store close", stage: alertlifecycle.RegroupStageFailurePersistence, reason: alertlifecycle.RegroupReasonFailurePersistenceStoreClosed, expected: true, want: slog.LevelDebug},
		{name: "external store close", stage: alertlifecycle.RegroupStageFailurePersistence, reason: alertlifecycle.RegroupReasonFailurePersistenceStoreClosed, want: slog.LevelWarn},
		{name: "hard durability failure", stage: alertlifecycle.RegroupStageBegin, reason: alertlifecycle.RegroupReasonCommitOutcomeUnknown, want: slog.LevelError},
		{name: "suffix is not a reason", stage: alertlifecycle.RegroupStageBatch, reason: "custom_cancelled", want: slog.LevelError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := regroupInfrastructureLogLevel(test.stage, test.reason, test.expected); got != test.want {
				t.Fatalf("severity: got=%s want=%s", got, test.want)
			}
		})
	}
}

func TestRegroupInfrastructureErrorValidatesExactStageReasonPairs(t *testing.T) {
	valid := &alertlifecycle.RegroupInfrastructureError{
		Stage:      alertlifecycle.RegroupStageBegin,
		ReasonCode: alertlifecycle.RegroupReasonCancelled,
	}
	if !valid.Valid() {
		t.Fatal("valid regroup infrastructure pair rejected")
	}
	invalid := &alertlifecycle.RegroupInfrastructureError{
		Stage:      alertlifecycle.RegroupStageBegin,
		ReasonCode: "custom_cancelled",
	}
	if invalid.Valid() {
		t.Fatal("unregistered regroup infrastructure reason accepted")
	}
}

func TestRegroupOwnerLogsDirectCancellationAtExpectedSeverity(t *testing.T) {
	previousLogger := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	logAlertLifecycleRegroupResult(context.Background(), alertlifecycle.GroupingConfig{}, time.Now(), alertlifecycle.RegroupResult{}, &alertlifecycle.RegroupInfrastructureError{
		Stage:            alertlifecycle.RegroupStageBatch,
		ReasonCode:       alertlifecycle.RegroupReasonCancelled,
		ExpectedShutdown: true,
		Err:              context.Canceled,
	})
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "DEBUG" || record["event"] != "regroup_cancelled" {
		t.Fatalf("expected shutdown cancellation log: %v", record)
	}
}

func TestRegroupOwnerLogsExternalCancellationAsWarning(t *testing.T) {
	previousLogger := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	logAlertLifecycleRegroupResult(context.Background(), alertlifecycle.GroupingConfig{}, time.Now(), alertlifecycle.RegroupResult{}, &alertlifecycle.RegroupInfrastructureError{
		Stage:      alertlifecycle.RegroupStageBatch,
		ReasonCode: alertlifecycle.RegroupReasonCancelled,
		Err:        context.Canceled,
	})
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "WARN" || record["event"] != "regroup_cancelled" {
		t.Fatalf("external cancellation log: %v", record)
	}
}

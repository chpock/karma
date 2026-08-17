package alertlifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestRecheckInfrastructureVocabularyUsesExactPairs(t *testing.T) {
	valid := []struct {
		stage  string
		reason string
	}{
		{stage: "batch", reason: "commit_not_committed"},
		{stage: "batch", reason: "writer_invariant"},
		{stage: "batch", reason: "rollback_failed"},
		{stage: "batch", reason: "worker_cancelled"},
		{stage: "batch", reason: "worker_deadline"},
		{stage: "failure_persistence", reason: "failure_persistence_commit_unknown"},
		{stage: "failure_persistence", reason: "failure_persistence_not_committed"},
		{stage: "failure_persistence", reason: "failure_persistence_sql_error"},
		{stage: "failure_persistence", reason: "failure_persistence_cancelled"},
	}
	for _, pair := range valid {
		if !validRecheckInfrastructurePair(pair.stage, pair.reason) {
			t.Fatalf("valid pair rejected: %s/%s", pair.stage, pair.reason)
		}
	}
	for _, pair := range [][2]string{{"batch", "custom_cancelled"}, {"failure_persistence", "failure_persistence_custom"}, {"batch", "failure_persistence_not_committed"}} {
		if validRecheckInfrastructurePair(pair[0], pair[1]) {
			t.Fatalf("invalid pair accepted: %s/%s", pair[0], pair[1])
		}
	}
}

func TestRecheckInfrastructureInvalidPairEmitsContractViolation(t *testing.T) {
	previousLogger := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	rootErr := errors.New("invalid reason")
	err := emitRecheckInfrastructureBlocked(context.Background(), 17, writerOperationRecheckBatch, "custom_cancelled", rootErr)
	if !errors.Is(err, ErrRecheckInfrastructureContract) || !errors.Is(err, ErrWriterOperationInvariant) {
		t.Fatalf("invalid pair error: %v", err)
	}
	if strings.Contains(output.String(), `"event":"job_infrastructure_blocked"`) || !strings.Contains(output.String(), `"event":"job_infrastructure_contract_violation"`) {
		t.Fatalf("invalid pair event output: %s", output.String())
	}
	var record map[string]any
	if err = json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "ERROR" || record["supplied_stage"] != "batch" || record["supplied_reason"] != "custom_cancelled" {
		t.Fatalf("contract violation fields: %v", record)
	}
}

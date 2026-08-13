package log

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestEventWritesMandatoryStructuredFieldsAndHonorsLevel(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	Event(context.Background(), slog.LevelDebug, "test_component", "hidden", "hidden message")
	Event(context.Background(), slog.LevelInfo, "test_component", "visible", "visible message", slog.String("item_id", "42"))

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("failed to decode log record: %v", err)
	}
	if record["component"] != "test_component" || record["event"] != "visible" || record["item_id"] != "42" {
		t.Fatalf("unexpected structured record: %+v", record)
	}
	if record["msg"] != "visible message" || record["level"] != "INFO" {
		t.Fatalf("unexpected message or level: %+v", record)
	}
}

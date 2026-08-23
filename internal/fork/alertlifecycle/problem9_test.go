package alertlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func TestProblem9GenericWriterObservabilityFailureIsSafeAndRegistered(t *testing.T) {
	tests := []struct {
		name      string
		stage     string
		safeError string
		run       func(*bytes.Buffer)
	}{
		{name: "handoff", stage: "after_commit_handoff", safeError: "writer_error", run: func(_ *bytes.Buffer) {
			runWriterFinalizer(&writerAfterCommitFinalizer{ctx: context.Background(), operationKind: writerOperationPoll, handoffErr: errors.New("raw handoff SQL secret")})
		}},
		{name: "finalizer panic", stage: "after_commit_run", safeError: "panic", run: func(_ *bytes.Buffer) {
			runWriterFinalizer(&writerAfterCommitFinalizer{ctx: context.Background(), operationKind: writerOperationPoll, run: func(bool) { panic("raw finalizer token") }})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previous) })
			test.run(&logs)
			assertProblem9WriterEvent(t, logs.String(), test.stage, test.safeError, test.name)
		})
	}

	t.Run("rollback recovery", func(t *testing.T) {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(previous) })
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "problem9.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		store.writerRollback = func(tx *sql.Tx) error {
			_ = tx.Rollback()
			return errors.New("raw rollback SQL secret")
		}
		store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) {
			return nil, errors.New("raw replacement path")
		}
		_, _ = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
			return struct{}{}, errors.New("raw callback SQL secret")
		})
		assertProblem9WriterEvent(t, logs.String(), "rollback_recovery", "writer_unavailable", "rollback recovery")
	})

	t.Run("panic rollback recovery", func(t *testing.T) {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(previous) })
		store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "problem9-panic.sqlite")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		rollbackErr := errors.New("raw panic rollback secret")
		replacementErr := errors.New("raw panic replacement secret")
		store.writerRollback = func(tx *sql.Tx) error {
			_ = tx.Rollback()
			return rollbackErr
		}
		store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) { return nil, replacementErr }
		func() {
			defer func() {
				if recovered := recover(); recovered != "raw original panic" {
					t.Fatalf("original panic changed: %v", recovered)
				}
			}()
			_, _ = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
				panic("raw original panic")
			})
		}()
		assertProblem9WriterEvent(t, logs.String(), "panic_rollback_recovery", "writer_unavailable", "panic rollback recovery")
	})
}

func assertProblem9WriterEvent(t *testing.T, raw, wantStage, wantSafeError, branch string) {
	t.Helper()
	if strings.Contains(raw, "raw ") || strings.Contains(raw, "SQL secret") || strings.Contains(raw, "token") {
		t.Fatalf("%s leaked raw writer diagnostic: %s", branch, raw)
	}
	matchingEvents := 0
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		event, _ := record["event"].(string)
		component, _ := record["component"].(string)
		if component == "lifecycle_writer" && event != "writer_observability_failed" {
			t.Fatalf("%s emitted an additional lifecycle-writer event=%v", branch, record)
		}
		if component == "silence_poll" || component == "silence_reconciliation" || component == "silence_recheck" {
			t.Fatalf("%s emitted a conflicting poll/recheck event=%v", branch, record)
		}
		if strings.Contains(event, "writer") && event != "writer_observability_failed" {
			t.Fatalf("%s emitted an additional generic-writer event=%v", branch, record)
		}
		if event != "writer_observability_failed" {
			continue
		}
		matchingEvents++
		if record["component"] != "lifecycle_writer" || record["level"] != "ERROR" || record["writer_operation"] != writerOperationPoll || record["stage"] != wantStage || record["safe_error"] != wantSafeError {
			t.Fatalf("%s writer event=%v", branch, record)
		}
	}
	if matchingEvents == 1 {
		return
	}
	if matchingEvents > 1 {
		t.Fatalf("%s emitted %d writer_observability_failed events, want exactly one: %s", branch, matchingEvents, raw)
	}
	t.Fatalf("%s did not emit registered writer_observability_failed: %s", branch, raw)
}

type problem9PanickingHandler struct{}

func (problem9PanickingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (problem9PanickingHandler) Handle(context.Context, slog.Record) error {
	panic("raw logging handler panic")
}

func (h problem9PanickingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h problem9PanickingHandler) WithGroup(string) slog.Handler { return h }

func TestProblem9PanickingLoggerCannotChangeWriterOutcomes(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(problem9PanickingHandler{}))
	t.Cleanup(func() { slog.SetDefault(previous) })

	runWriterFinalizer(&writerAfterCommitFinalizer{ctx: context.Background(), operationKind: writerOperationPoll, handoffErr: errors.New("handoff failure")})
	runWriterFinalizer(&writerAfterCommitFinalizer{ctx: context.Background(), operationKind: writerOperationPoll, run: func(bool) { panic("original finalizer panic") }})

	store, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "problem9-handler.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	callbackErr := errors.New("callback failure")
	rollbackErr := errors.New("rollback failure")
	replacementErr := errors.New("replacement failure")
	store.writerRollback = func(tx *sql.Tx) error {
		_ = tx.Rollback()
		return rollbackErr
	}
	store.writerConnectionAcquirer = func(context.Context) (*sql.Conn, error) { return nil, replacementErr }
	_, err = withWriterTxValue(context.Background(), store, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
		return struct{}{}, callbackErr
	})
	if !errors.Is(err, callbackErr) || !errors.Is(err, rollbackErr) || !errors.Is(err, ErrWriterUnavailable) || !errors.Is(err, replacementErr) {
		t.Fatalf("panicking logger changed rollback outcome: %v", err)
	}

	panicStore, err := OpenStore(StoreOptions{Path: filepath.Join(t.TempDir(), "problem9-handler-panic.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = panicStore.Close() })
	panicStore.writerRollback = store.writerRollback
	panicStore.writerConnectionAcquirer = store.writerConnectionAcquirer
	func() {
		defer func() {
			if recovered := recover(); recovered != "original writer panic" {
				t.Fatalf("panicking logger changed original panic: %v", recovered)
			}
		}()
		_, _ = withWriterTxValue(context.Background(), panicStore, writerOperationPoll, func(*sql.Tx) (struct{}, error) {
			panic("original writer panic")
		})
	}()
}

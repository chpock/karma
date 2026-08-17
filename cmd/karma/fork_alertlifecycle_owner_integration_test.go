package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestRealRegroupOwnerStartsAndJoinsWorker(t *testing.T) {
	previousStore := lifecycleStore
	previousConfig := config.Config
	t.Cleanup(func() {
		lifecycleStore = previousStore
		config.Config = previousConfig
	})
	previousDependencies := regroupOwnerDependencies
	t.Cleanup(func() { regroupOwnerDependencies = previousDependencies })
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/history.sqlite"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lifecycleStore = store
	config.Config.AlertLifecycle.Enabled = true
	config.Config.Labels.Keep = []string{"alertname"}
	now := time.Now().UTC()
	if err = store.UpsertCurrentAlert(alertlifecycle.AlertCurrent{AlertID: "owner-regroup-alert", State: alertlifecycle.StateInactive, RawState: alertlifecycle.StateActive, Receiver: "team-a", GroupID: "old-group", LabelsJSON: `{"alertname":"Owner"}`, AnnotationsJSON: `{}`, StartsAt: now.Add(-time.Hour), LastSeenAt: &now, RelevantUpstreams: `[]`}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(&blockingEventHandler{next: slog.NewTextHandler(io.Discard, nil), entered: entered, release: release}))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	cancelCalled := make(chan struct{})
	waitCalled := make(chan struct{})
	regroupOwnerDependencies = func(cancel context.CancelCauseFunc, _ <-chan struct{}) regroupOwnerDeps {
		return regroupOwnerDeps{
			cancel: func(cause error) { close(cancelCalled); cancel(cause) },
			waitDone: func(done <-chan struct{}) {
				close(waitCalled)
				<-done
			},
		}
	}
	startAlertLifecycleBackgroundRegroup()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real regroup owner did not emit regroup_started")
	}
	stopDone := make(chan struct{})
	go func() {
		stopAlertLifecycleBackgroundRegroup()
		close(stopDone)
	}()
	<-cancelCalled
	<-waitCalled
	close(release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("real regroup owner did not join the worker")
	}
}

func TestRealHousekeepingOwnerStartsAndJoinsWorkerIncludingRecheckPhase(t *testing.T) {
	previousStore := lifecycleStore
	previousConfig := config.Config
	t.Cleanup(func() {
		lifecycleStore = previousStore
		config.Config = previousConfig
	})
	previousDependencies := housekeepingOwnerDependencies
	previousRunner := runAlertLifecycleRecheckForHousekeeping
	t.Cleanup(func() {
		housekeepingOwnerDependencies = previousDependencies
		runAlertLifecycleRecheckForHousekeeping = previousRunner
	})
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: t.TempDir() + "/history.sqlite"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lifecycleStore = store
	config.Config.AlertLifecycle.Enabled = true
	config.Config.AlertLifecycle.Retention.TTL = time.Hour
	config.Config.AlertLifecycle.Retention.MaxAlerts = 100
	config.Config.AlertLifecycle.SilenceHistory.Retention.TTL = 24 * time.Hour
	config.Config.AlertLifecycle.Housekeeping.IncrementalVacuumInterval = time.Hour
	entered := make(chan struct{})
	release := make(chan struct{})
	runAlertLifecycleRecheckForHousekeeping = func(ctx context.Context) {
		previousRunner(ctx)
		close(entered)
		<-release
	}
	cancelCalled := make(chan struct{})
	waitCalled := make(chan struct{})
	housekeepingOwnerDependencies = func(cancel context.CancelFunc, _ <-chan struct{}) lifecycleWorkerDeps {
		return lifecycleWorkerDeps{
			cancel: func(error) { close(cancelCalled); cancel() },
			waitDone: func(done <-chan struct{}) {
				close(waitCalled)
				<-done
			},
		}
	}
	startAlertLifecycleHousekeeping()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real housekeeping owner did not reach synchronous recheck")
	}
	stopDone := make(chan struct{})
	go func() {
		stopAlertLifecycleHousekeeping()
		close(stopDone)
	}()
	<-cancelCalled
	<-waitCalled
	close(release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("real housekeeping owner did not join synchronous recheck")
	}
}

type blockingEventHandler struct {
	next    slog.Handler
	entered chan struct{}
	release <-chan struct{}
}

func (handler *blockingEventHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.next.Enabled(ctx, level)
}

func (handler *blockingEventHandler) Handle(ctx context.Context, record slog.Record) error {
	event := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event" {
			event = attr.Value.String()
			return false
		}
		return true
	})
	if event == "regroup_started" {
		close(handler.entered)
		<-handler.release
	}
	return handler.next.Handle(ctx, record)
}

func (handler *blockingEventHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &blockingEventHandler{next: handler.next.WithAttrs(attrs), entered: handler.entered, release: handler.release}
}

func (handler *blockingEventHandler) WithGroup(name string) slog.Handler {
	return &blockingEventHandler{next: handler.next.WithGroup(name), entered: handler.entered, release: handler.release}
}

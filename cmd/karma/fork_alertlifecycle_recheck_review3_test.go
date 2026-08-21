package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/prymitive/karma/internal/config"
	"github.com/prymitive/karma/internal/fork/alertlifecycle"
)

func TestProblem3Review3CommitUnknownOwnsBookkeepingReconciliationBackoff(t *testing.T) {
	clock := &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 9, 0, 0, 0, time.UTC)}
	scheduler := newRecheckScheduler(nil, clock)
	timer := clock.NewTimer(time.Second)
	bookkeepingErr := alertlifecycle.NewRecheckFailurePersistenceError(errors.New("review3 bookkeeping unavailable"), true)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, bookkeepingErr
	}
	scheduler.process(timer, nil)
	scheduler.process(timer, nil)

	joined := errors.Join(bookkeepingErr, alertlifecycle.ErrWriterCommitOutcomeUnknown, context.DeadlineExceeded)
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		return alertlifecycle.RecheckBatchResult{}, joined
	}
	scheduler.process(timer, nil)
	want := []time.Duration{time.Second, 5 * time.Second, time.Second}
	if len(clock.timer.resets) != len(want) {
		t.Fatalf("backoff resets=%v want=%v", clock.timer.resets, want)
	}
	for i := range want {
		if clock.timer.resets[i] != want[i] {
			t.Fatalf("backoff[%d]=%s want=%s all=%v", i, clock.timer.resets[i], want[i], clock.timer.resets)
		}
	}
	if scheduler.backoffStep[recheckBackoffBookkeeping] != 2 || scheduler.backoffStep[recheckBackoffReconciliation] != 1 {
		t.Fatalf("owner steps=%v", scheduler.backoffStep)
	}
}

func TestProblem3Review3RegisteredRouterOperatorContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	result, err := store.ApplyPollCycle(alertlifecycle.PollCycleInput{
		SuccessfulUpstreams: map[string]bool{"review3-am": true},
		SilenceSources: []alertlifecycle.SourceSilenceState{{
			SourceName: "review3-am", ClusterID: "review3-cluster", SyncStatus: "success",
			Silences: []alertlifecycle.SourceSilenceSnapshot{{
				SilenceID: "review3-silence", Status: "active", StartsAt: now.Add(-time.Hour), EndsAt: now.Add(time.Hour),
				Matchers: []alertlifecycle.SilenceMatcherSnapshot{{Name: "service", Value: "review3", IsEqual: true}},
			}},
		}},
		Timestamp: now,
	})
	if err != nil || result.RecheckJobsQueued != 1 {
		t.Fatalf("job creation result=%+v err=%v", result, err)
	}
	if _, err = store.RunSilenceRecheckBatchContext(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	finished := now.Add(time.Second).Format(time.RFC3339Nano)
	if _, err = db.Exec(`UPDATE silence_recheck_jobs SET status='failed',started_at=COALESCE(started_at,?),finished_at=? WHERE job_id=1`, now.Format(time.RFC3339Nano), finished); err != nil {
		t.Fatal(err)
	}

	previousStore := lifecycleStore
	previousConfig := config.Config
	lifecycleStore = store
	config.Config.Listen.Prefix = "/review3"
	config.Config.Authentication.Enabled = true
	t.Cleanup(func() {
		lifecycleStore = previousStore
		config.Config = previousConfig
	})
	router := chi.NewRouter()
	registerAlertLifecycleRecheckRoutes(router)
	boundaryRequest := func(body, contentType, marker, fetchSite string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/review3/alerts-history/recheck/1/retry", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", contentType)
		if marker != "" {
			req.Header.Set(recheckOperatorRequestHeader, marker)
		}
		if fetchSite != "" {
			req.Header.Set("Sec-Fetch-Site", fetchSite)
		}
		return req.WithContext(context.WithValue(req.Context(), authUserKey("user"), "attacker"))
	}
	for name, request := range map[string]*http.Request{
		"missing marker": boundaryRequest(`{"requestId":"hostile-missing"}`, "application/json", "", "same-origin"),
		"cross-site":     boundaryRequest(`{"requestId":"hostile-cross-site"}`, "application/json", "recheck-operator", "cross-site"),
		"simple text":    boundaryRequest(`{"requestId":"hostile-text"}`, "text/plain", "recheck-operator", "same-origin"),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			want := http.StatusForbidden
			if name == "simple text" {
				want = http.StatusUnsupportedMediaType
			}
			if recorder.Code != want {
				t.Fatalf("boundary status=%d want=%d body=%s", recorder.Code, want, recorder.Body.String())
			}
		})
	}

	request := func(path, body, actor string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(recheckOperatorRequestHeader, "recheck-operator")
		return req.WithContext(context.WithValue(req.Context(), authUserKey("user"), actor))
	}
	retry := httptest.NewRecorder()
	router.ServeHTTP(retry, request("/review3/alerts-history/recheck/1/retry", `{"requestId":"review3-retry","reason":"retry now"}`, "alice"))
	if retry.Code != http.StatusOK || !strings.Contains(retry.Body.String(), `"currentStatus":"queued"`) {
		t.Fatalf("registered retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	var retryActor, retryAction string
	if err = db.QueryRow(`SELECT actor,action FROM silence_recheck_job_operator_actions WHERE request_id='review3-retry'`).Scan(&retryActor, &retryAction); err != nil {
		t.Fatal(err)
	}
	if retryActor != "alice" || retryAction != "retry" {
		t.Fatalf("retry actor/action=%q/%q", retryActor, retryAction)
	}

	if _, err = db.Exec(`UPDATE silence_recheck_jobs SET status='failed',started_at=?,finished_at=? WHERE job_id=1`, now.Format(time.RFC3339Nano), finished); err != nil {
		t.Fatal(err)
	}
	abandon := httptest.NewRecorder()
	router.ServeHTTP(abandon, request("/review3/alerts-history/recheck/1/abandon", `{"requestId":"review3-abandon","reason":"operator decision"}`, "bob"))
	if abandon.Code != http.StatusOK || !strings.Contains(abandon.Body.String(), `"currentStatus":"abandoned"`) {
		t.Fatalf("registered abandon status=%d body=%s", abandon.Code, abandon.Body.String())
	}
	var abandonActor, abandonAction string
	if err = db.QueryRow(`SELECT actor,action FROM silence_recheck_job_operator_actions WHERE request_id='review3-abandon'`).Scan(&abandonActor, &abandonAction); err != nil {
		t.Fatal(err)
	}
	if abandonActor != "bob" || abandonAction != "abandon" {
		t.Fatalf("abandon actor/action=%q/%q", abandonActor, abandonAction)
	}
}

func TestProblem3Review3HousekeepingDoesNotWakeRecheckScheduler(t *testing.T) {
	store, err := alertlifecycle.OpenStore(alertlifecycle.StoreOptions{Path: filepath.Join(t.TempDir(), "history.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	previousStore := lifecycleStore
	previousScheduler := lifecycleRecheckScheduler
	previousConfig := config.Config
	lifecycleStore = store
	scheduler := newRecheckScheduler(store, nil)
	lifecycleRecheckScheduler = scheduler
	config.Config.AlertLifecycle.Retention.TTL = time.Hour
	config.Config.AlertLifecycle.Retention.MaxAlerts = 100
	config.Config.AlertLifecycle.SilenceHistory.Retention.TTL = 24 * time.Hour
	t.Cleanup(func() {
		lifecycleStore = previousStore
		lifecycleRecheckScheduler = previousScheduler
		config.Config = previousConfig
	})

	runAlertLifecycleHousekeepingOnce(context.Background())
	if got := len(scheduler.wake); got != 0 {
		t.Fatalf("housekeeping produced ambient scheduler wakes=%d", got)
	}
}

func TestProblem3Review3TerminalDispositionStopsSchedulerOwner(t *testing.T) {
	scheduler := newRecheckScheduler(nil, &reviewSchedulerClock{now: time.Date(2026, time.August, 19, 10, 0, 0, 0, time.UTC)})
	calls := make(chan int, 2)
	call := 0
	scheduler.runner = func(context.Context, int) (alertlifecycle.RecheckBatchResult, error) {
		call++
		calls <- call
		return alertlifecycle.RecheckBatchResult{}, errors.Join(alertlifecycle.ErrWriterOperationInvariant, errors.New("review3 terminal invariant"))
	}
	scheduler.Start()
	if first := <-calls; first != 1 {
		t.Fatalf("first runner call=%d", first)
	}
	scheduler.Signal()
	select {
	case <-scheduler.done:
	case second := <-calls:
		t.Fatalf("terminal scheduler accepted later wake: call=%d", second)
	}
	scheduler.Signal()
	select {
	case second := <-calls:
		t.Fatalf("stopped scheduler ran after completion: call=%d", second)
	default:
	}
}

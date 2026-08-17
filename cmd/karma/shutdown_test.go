package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestApplicationShutdownDrainsHTTPBeforeHistoryAndStore(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerReturned := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(handlerStarted)
		<-releaseHandler
		close(handlerReturned)
	}))
	defer server.Close()
	requestDone := make(chan struct{})
	go func() {
		response, _ := http.Get(server.URL)
		if response != nil {
			_ = response.Body.Close()
		}
		close(requestDone)
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	var mu sync.Mutex
	order := make([]string, 0, 8)
	record := func(name string) { mu.Lock(); order = append(order, name); mu.Unlock() }
	shutdownStarted := make(chan struct{})
	orderError := make(chan error, 2)
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- (applicationShutdown{
			markPollStopping: func() { record("poll_mark") },
			stopTicker:       func() { record("ticker") },
			closePollStop:    func() { record("poll_stop") },
			waitPoll:         func() { record("poll_wait") },
			newHTTPContext: func() (context.Context, context.CancelFunc) {
				record("http_context")
				return context.WithCancel(context.Background())
			},
			shutdownHTTP: func(ctx context.Context) error {
				record("http_shutdown")
				close(shutdownStarted)
				return server.Config.Shutdown(ctx)
			},
			closeHTTP: func() error { record("http_close"); return nil },
			stopHistory: func() {
				select {
				case <-handlerReturned:
				default:
					orderError <- errors.New("history poller stopped before handler returned")
					return
				}
				record("history")
			},
			stopRegroup:      func() { record("regroup") },
			stopHousekeeping: func() { record("housekeeping") },
			closeStore: func() error {
				select {
				case <-handlerReturned:
				default:
					orderError <- errors.New("store closed before handler returned")
					return nil
				}
				record("store")
				return nil
			},
		}).run()
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP shutdown did not start")
	}
	select {
	case <-handlerReturned:
		t.Fatal("handler returned before release")
	default:
	}
	record("release_handler")
	close(releaseHandler)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not finish")
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
	select {
	case err := <-orderError:
		t.Fatal(err)
	default:
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{"poll_mark", "ticker", "poll_stop", "poll_wait", "http_context", "http_shutdown", "release_handler", "history", "housekeeping", "regroup", "store"}
	if len(got) != len(want) {
		t.Fatalf("shutdown order length: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("shutdown order: got=%v want=%v", got, want)
		}
	}
}

func TestApplicationShutdownTimeoutDoesNotCloseHistoryOrStore(t *testing.T) {
	shutdownErr := errors.New("shutdown deadline exceeded")
	var historyStopped, storeClosed, httpClosed bool
	err := (applicationShutdown{
		markPollStopping: func() {}, stopTicker: func() {}, closePollStop: func() {}, waitPoll: func() {},
		newHTTPContext:   func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		shutdownHTTP:     func(context.Context) error { return shutdownErr },
		closeHTTP:        func() error { httpClosed = true; return nil },
		stopHistory:      func() { historyStopped = true },
		stopRegroup:      func() { t.Fatal("regroup stopped after failed HTTP shutdown") },
		stopHousekeeping: func() { t.Fatal("housekeeping stopped after failed HTTP shutdown") },
		closeStore:       func() error { storeClosed = true; return nil },
	}).run()
	if !errors.Is(err, shutdownErr) || !httpClosed || historyStopped || storeClosed {
		t.Fatalf("shutdown timeout cleanup: err=%v httpClosed=%v historyStopped=%v storeClosed=%v", err, httpClosed, historyStopped, storeClosed)
	}
}

func TestApplicationShutdownPropagatesStoreCloseError(t *testing.T) {
	closeErr := errors.New("store close failed")
	err := (applicationShutdown{
		markPollStopping: func() {}, stopTicker: func() {}, closePollStop: func() {}, waitPoll: func() {},
		newHTTPContext: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		shutdownHTTP:   func(context.Context) error { return nil },
		closeHTTP:      func() error { return nil },
		stopHistory:    func() {}, stopHousekeeping: func() {}, stopRegroup: func() {},
		closeStore: func() error { return closeErr },
	}).run()
	if !errors.Is(err, closeErr) {
		t.Fatalf("store close error was not propagated: %v", err)
	}
}

func TestStartupCleanupJoinsPIDAndStoreErrors(t *testing.T) {
	releaseErr := errors.New("PID release failed")
	storeErr := errors.New("store close failed")
	err := (startupCleanup{
		releasePID: func() error { return releaseErr },
		closeStore: func() error { return storeErr },
	}).run()
	if !errors.Is(err, releaseErr) || !errors.Is(err, storeErr) {
		t.Fatalf("startup cleanup errors: %v", err)
	}
}

func TestStartupFailureJoinsOriginalPIDAndStoreErrors(t *testing.T) {
	startupErr := errors.New("startup failed")
	releaseErr := errors.New("PID release failed")
	storeErr := errors.New("store close failed")
	err := joinStartupCleanupError(startupErr, startupCleanup{
		releasePID: func() error { return releaseErr },
		closeStore: func() error { return storeErr },
	})
	if !errors.Is(err, startupErr) || !errors.Is(err, releaseErr) || !errors.Is(err, storeErr) {
		t.Fatalf("startup errors: %v", err)
	}
}

func TestApplicationShutdownJoinsStoreAndPIDReleaseErrors(t *testing.T) {
	storeErr := errors.New("store close failed")
	releaseErr := errors.New("PID release failed")
	err := (applicationShutdown{
		markPollStopping: func() {}, stopTicker: func() {}, closePollStop: func() {}, waitPoll: func() {},
		newHTTPContext: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		shutdownHTTP:   func(context.Context) error { return nil },
		closeHTTP:      func() error { return nil },
		stopHistory:    func() {}, stopHousekeeping: func() {}, stopRegroup: func() {},
		closeStore: func() error { return storeErr },
		releasePID: func() error { return releaseErr },
	}).run()
	if !errors.Is(err, storeErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("shutdown errors: %v", err)
	}
}

func TestApplicationShutdownHTTPErrorStillReleasesPID(t *testing.T) {
	httpErr := errors.New("HTTP shutdown failed")
	closeErr := errors.New("HTTP close failed")
	releaseErr := errors.New("PID release failed")
	err := (applicationShutdown{
		markPollStopping: func() {}, stopTicker: func() {}, closePollStop: func() {}, waitPoll: func() {},
		newHTTPContext: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		shutdownHTTP:   func(context.Context) error { return httpErr },
		closeHTTP:      func() error { return closeErr },
		releasePID:     func() error { return releaseErr },
	}).run()
	if !errors.Is(err, httpErr) || !errors.Is(err, closeErr) || !errors.Is(err, releaseErr) {
		t.Fatalf("HTTP shutdown errors: %v", err)
	}
}

func TestStartupCleanupReleasesResourcesInReverseAcquisitionOrder(t *testing.T) {
	order := make([]string, 0, 5)
	cleanup := startupCleanup{
		stopHistory:      func() { order = append(order, "history") },
		releasePID:       func() error { order = append(order, "pid"); return nil },
		stopHousekeeping: func() { order = append(order, "housekeeping") },
		stopRegroup:      func() { order = append(order, "regroup") },
		closeStore:       func() error { order = append(order, "store"); return nil },
	}
	if err := cleanup.run(); err != nil {
		t.Fatal(err)
	}
	want := []string{"history", "pid", "housekeeping", "regroup", "store"}
	if len(order) != len(want) {
		t.Fatalf("startup cleanup order length: got=%v want=%v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("startup cleanup order: got=%v want=%v", order, want)
		}
	}
}

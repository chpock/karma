package main

import (
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/prymitive/karma/internal/alertmanager"
)

const (
	maxTries = 2
)

type alertPollLoop struct {
	mu       sync.Mutex
	stopping bool
}

func newAlertPollLoop() *alertPollLoop {
	return &alertPollLoop{}
}

func (p *alertPollLoop) beginPoll() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.stopping
}

func (p *alertPollLoop) stop() {
	p.mu.Lock()
	p.stopping = true
	p.mu.Unlock()
}

func pullFromAlertmanager() {
	// always flush cache once we're done
	defer apiCache.Purge()

	slog.Info("Pulling latest alerts and silences from Alertmanager")

	upstreams := alertmanager.GetAlertmanagers()
	type pullResult struct {
		upstream string
		err      error
	}
	results := make(chan pullResult, len(upstreams))
	successfulUpstreams := make(map[string]bool, len(upstreams))
	wg := sync.WaitGroup{}
	wg.Add(len(upstreams))

	for _, upstream := range upstreams {
		go func(am *alertmanager.Alertmanager) {
			slog.Info("Collecting alerts and silences", slog.String("alertmanager", am.Name))
			var finalErr error
			for i := 1; i <= maxTries; i++ {
				err := am.Pull()
				if err != nil {
					finalErr = err
					slog.Error(
						"Collection failed",
						slog.Any("error", err),
						slog.String("alertmanager", am.Name),
						slog.String("try", fmt.Sprintf("%d/%d", i, maxTries)),
					)
				} else {
					finalErr = nil
					break
				}
			}
			results <- pullResult{upstream: am.Name, err: finalErr}
			wg.Done()
		}(upstream)
	}

	wg.Wait()
	close(results)

	for result := range results {
		successfulUpstreams[result.upstream] = result.err == nil
	}

	runAlertLifecyclePollCycle(successfulUpstreams)

	slog.Info("Collection completed")
	runtime.GC()
}

// Tick is the background timer used to call PullFromAlertmanager

func Tick(loop *alertPollLoop, ticks <-chan time.Time, stop <-chan struct{}, done chan<- struct{}, pull func()) {
	defer close(done)
	for {
		select {
		case <-ticks:
			if !loop.beginPoll() {
				return
			}
			pull()
		case <-stop:
			return
		}
	}
}

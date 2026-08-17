package main

import (
	"context"
	"sync"
)

type lifecycleWorkerDeps struct {
	cancel   func(error)
	waitDone func(<-chan struct{})
}

type lifecycleWorkerState uint8

const (
	lifecycleWorkerStopped lifecycleWorkerState = iota
	lifecycleWorkerRunning
	lifecycleWorkerStopping
)

type lifecycleWorkerOwner struct {
	mu          sync.Mutex
	state       lifecycleWorkerState
	deps        lifecycleWorkerDeps
	done        chan struct{}
	stoppedDone chan struct{}
}

var regroupOwnerDependencies = func(cancel context.CancelCauseFunc, _ <-chan struct{}) regroupOwnerDeps {
	return regroupOwnerDeps{
		cancel:   cancel,
		waitDone: func(done <-chan struct{}) { <-done },
	}
}

var housekeepingOwnerDependencies = func(cancel context.CancelFunc, _ <-chan struct{}) lifecycleWorkerDeps {
	return lifecycleWorkerDeps{
		cancel:   func(error) { cancel() },
		waitDone: func(done <-chan struct{}) { <-done },
	}
}

func (owner *lifecycleWorkerOwner) start(done chan struct{}, deps lifecycleWorkerDeps) bool {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.state != lifecycleWorkerStopped || done == nil || deps.cancel == nil {
		return false
	}
	if deps.waitDone == nil {
		deps.waitDone = func(done <-chan struct{}) { <-done }
	}
	owner.state = lifecycleWorkerRunning
	owner.deps = deps
	owner.done = done
	owner.stoppedDone = make(chan struct{})
	return true
}

func (owner *lifecycleWorkerOwner) stop(cause error) {
	owner.mu.Lock()
	switch owner.state {
	case lifecycleWorkerStopped:
		owner.mu.Unlock()
		return
	case lifecycleWorkerStopping:
		stoppedDone := owner.stoppedDone
		owner.mu.Unlock()
		<-stoppedDone
		return
	case lifecycleWorkerRunning:
		owner.state = lifecycleWorkerStopping
		deps := owner.deps
		done := owner.done
		stoppedDone := owner.stoppedDone
		owner.mu.Unlock()

		deps.cancel(cause)
		deps.waitDone(done)

		owner.mu.Lock()
		if owner.state == lifecycleWorkerStopping {
			owner.state = lifecycleWorkerStopped
			owner.deps = lifecycleWorkerDeps{}
			owner.done = nil
			owner.stoppedDone = nil
			close(stoppedDone)
		}
		owner.mu.Unlock()
	}
}

type (
	regroupOwnerDeps = lifecycleWorkerDeps
	regroupOwner     = lifecycleWorkerOwner
)

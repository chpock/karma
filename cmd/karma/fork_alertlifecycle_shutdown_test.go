package main

import (
	"errors"
	"testing"
)

func TestRegroupOwnerStopJoinsWorkerWithoutElapsedOrderingProof(t *testing.T) {
	owner := &regroupOwner{}
	done := make(chan struct{})
	cancelCalled := make(chan struct{})
	waitCalled := make(chan struct{})
	stopReturned := make(chan struct{})
	if !owner.start(done, regroupOwnerDeps{
		cancel: func(error) { close(cancelCalled) },
		waitDone: func(done <-chan struct{}) {
			close(waitCalled)
			<-done
		},
	}) {
		t.Fatal("owner did not start")
	}
	go func() {
		owner.stop(errors.New("test stop"))
		close(stopReturned)
	}()
	<-cancelCalled
	<-waitCalled
	select {
	case <-stopReturned:
		t.Fatal("owner stop returned before worker completion")
	default:
	}
	close(done)
	<-stopReturned
	owner.stop(nil)
}

func TestRegroupOwnerConcurrentStopsJoinOnce(t *testing.T) {
	owner := &regroupOwner{}
	done := make(chan struct{})
	cancelCalls := make(chan struct{}, 2)
	waitCalls := make(chan struct{}, 2)
	if !owner.start(done, regroupOwnerDeps{
		cancel: func(error) { cancelCalls <- struct{}{} },
		waitDone: func(done <-chan struct{}) {
			waitCalls <- struct{}{}
			<-done
		},
	}) {
		t.Fatal("owner did not start")
	}
	finished := make(chan struct{}, 2)
	go func() { owner.stop(nil); finished <- struct{}{} }()
	go func() { owner.stop(nil); finished <- struct{}{} }()
	<-cancelCalls
	<-waitCalls
	close(done)
	<-finished
	<-finished
	select {
	case <-cancelCalls:
		t.Fatal("concurrent stop invoked cancel twice")
	default:
	}
	select {
	case <-waitCalls:
		t.Fatal("concurrent stop joined twice")
	default:
	}
}

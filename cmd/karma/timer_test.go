package main

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestTickStopsAndJoinsWithoutStartingAnotherPoll(t *testing.T) {
	loop := newAlertPollLoop()
	ticks := make(chan time.Time)
	stop := make(chan struct{})
	done := make(chan struct{})
	go Tick(loop, ticks, stop, done, func() { t.Fatal("poll started after stop") })
	loop.stop()
	close(stop)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Tick did not stop after shutdown signal")
	}
}

func TestTickDoesNotStartReadyTickAfterStopArbitration(t *testing.T) {
	loop := newAlertPollLoop()
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	stop := make(chan struct{})
	done := make(chan struct{})
	var polls atomic.Int32
	loop.stop()
	go Tick(loop, ticks, stop, done, func() { polls.Add(1) })
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Tick did not stop with a ready tick")
	}
	if polls.Load() != 0 {
		t.Fatalf("ready tick started after stop arbitration: polls=%d", polls.Load())
	}
}

func TestTickWaitsForInFlightPollBeforeDone(t *testing.T) {
	loop := newAlertPollLoop()
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	stop := make(chan struct{})
	done := make(chan struct{})
	pollStarted := make(chan struct{})
	release := make(chan struct{})
	go Tick(loop, ticks, stop, done, func() {
		close(pollStarted)
		<-release
	})
	select {
	case <-pollStarted:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	loop.stop()
	close(stop)
	select {
	case <-done:
		t.Fatal("Tick finished before in-flight poll returned")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Tick did not join in-flight poll")
	}
}

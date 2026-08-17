//go:build !windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestManagedKarmaStartupTimeoutReapsLiveChildWithoutPID(t *testing.T) {
	dir := t.TempDir()
	childScript := filepath.Join(dir, "karma")
	if err := os.WriteFile(childScript, []byte("#!/bin/sh\ntrap '' TERM INT HUP QUIT\nprintf started >&3\nwhile :; do sleep 1; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(dir, "state")
	pidFile := filepath.Join(dir, "missing.pid")
	startedRead, startedWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = startedRead.Close()
		_ = startedWrite.Close()
	})
	startupDeadline := make(chan time.Time, 1)
	readinessTicks := make(chan time.Time)
	var child *exec.Cmd
	deps := managedKarmaDependencies{
		resolveExecutable: func() (string, error) { return childScript, nil },
		command: func(executable string, args []string) *exec.Cmd {
			child = exec.Command(executable, args...)
			child.ExtraFiles = []*os.File{startedWrite}
			return child
		},
		newTimer: func(time.Duration) managedTimerSource {
			return managedTimerSource{C: startupDeadline, Stop: func() bool { return true }}
		},
		newTicker: func(time.Duration) managedTickerSource {
			return managedTickerSource{C: readinessTicks, Stop: func() {}}
		},
	}

	result := make(chan int, 1)
	go func() {
		result <- runManagedKarmaWithDependencies([]string{stateDir, pidFile, "25ms", "25ms", "karma", "--pid-file=" + pidFile}, deps)
	}()
	startedResult := make(chan error, 1)
	go func() {
		started := make([]byte, 1)
		_, readErr := startedRead.Read(started)
		if readErr == nil && string(started) != "s" {
			readErr = errors.New("unexpected child start signal: " + string(started))
		}
		startedResult <- readErr
	}()
	select {
	case err = <-startedResult:
		if err != nil {
			t.Fatalf("managed child did not report causal start: %v", err)
		}
	case code := <-result:
		t.Fatalf("managed child exited before causal start: code=%d", code)
	case <-time.After(2 * time.Second):
		t.Fatal("managed child did not start")
	}
	_ = startedWrite.Close()
	startupDeadline <- time.Now()

	select {
	case code := <-result:
		if code != managedTimeoutCode {
			t.Fatalf("managed timeout result: got=%d want=%d", code, managedTimeoutCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("managed startup timeout did not terminate and reap the child")
	}
	if child == nil || child.ProcessState == nil {
		t.Fatalf("managed child was not reaped: child=%v state=%v", child, child.ProcessState)
	}
	status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("managed child was not force-killed after ignored graceful signal: status=%v", child.ProcessState)
	}
	if err = child.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("managed child still appears live after reap: %v", err)
	}
	startup, err := os.ReadFile(filepath.Join(stateDir, "startup"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(startup), "failed:timeout:") {
		t.Fatalf("startup timeout marker: %q", startup)
	}
	harnessError, err := os.ReadFile(filepath.Join(stateDir, "harness-error"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(harnessError), "startup timeout") {
		t.Fatalf("startup timeout diagnostic: %q", harnessError)
	}
}

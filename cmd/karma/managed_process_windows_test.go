//go:build windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"time"
)

func configureManagedProcess(_ *exec.Cmd) {}

func managedNotifySignals(ch chan<- os.Signal) { signal.Notify(ch, os.Interrupt) }

func managedStopSignals(ch chan<- os.Signal) { signal.Stop(ch) }

func terminateManagedProcess(cmd *exec.Cmd, waitResult <-chan error, _ time.Duration) (string, error) {
	if cmd.Process == nil {
		return "signal:unknown", errors.New("managed process has no process handle")
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return "signal:unknown", err
	}
	if waitResult == nil {
		return "signal:unknown", nil
	}
	err := <-waitResult
	return managedStatusToken(err), nil
}

func cleanupManagedProcessGroup(_ int, _ time.Duration) error { return nil }

func managedSignalToken(_ error) string { return "signal:unknown" }

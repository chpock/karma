//go:build !windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func configureManagedProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	managedSetParentDeathSignal(cmd)
}

func managedNotifySignals(ch chan<- os.Signal) {
	signalNotify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
}

func managedStopSignals(ch chan<- os.Signal) {
	signalStop(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
}

func terminateManagedProcess(cmd *exec.Cmd, waitResult <-chan error, grace time.Duration) (string, error) {
	if cmd.Process == nil {
		return "signal:unknown", errors.New("managed process has no process handle")
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return "signal:unknown", err
	}
	if waitResult == nil {
		return "signal:unknown", nil
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case err := <-waitResult:
		return managedStatusToken(err), nil
	case <-timer.C:
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return "signal:unknown", err
		}
		err := <-waitResult
		return managedStatusToken(err), nil
	}
}

func cleanupManagedProcessGroup(pid int, grace time.Duration) error {
	return cleanupManagedProcessGroupWithProbe(pid, grace, syscall.Kill)
}

func cleanupManagedProcessGroupWithProbe(pid int, grace time.Duration, probe func(int, syscall.Signal) error) error {
	initialErr := probe(-pid, 0)
	if initialErr != nil {
		if errors.Is(initialErr, syscall.ESRCH) {
			return nil
		}
		return initialErr
	}
	if killErr := probe(-pid, syscall.SIGTERM); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
		return killErr
	}
	if err := waitManagedProcessGroupGone(pid, grace, probe); err == nil {
		return nil
	} else if !errors.Is(err, errManagedProcessGroupStillPresent) {
		return err
	}
	if killErr := probe(-pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
		return killErr
	}
	return waitManagedProcessGroupGone(pid, grace, probe)
}

var errManagedProcessGroupStillPresent = errors.New("managed process group did not disappear")

func waitManagedProcessGroupGone(pid int, timeout time.Duration, probe func(int, syscall.Signal) error) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		err := probe(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-timer.C:
			return errManagedProcessGroupStillPresent
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func signalNotify(ch chan<- os.Signal, signals ...os.Signal) {
	// Kept in a small helper so the test-only launcher has one signal boundary.
	signal.Notify(ch, signals...)
}

func signalStop(ch chan<- os.Signal, _ ...os.Signal) {
	signal.Stop(ch)
}

func managedSignalToken(err error) string {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return "signal:unknown"
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if ok && status.Signaled() {
		return "signal:" + status.Signal().String()
	}
	return "signal:unknown"
}

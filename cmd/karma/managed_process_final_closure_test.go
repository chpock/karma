//go:build !windows

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestManagedProcessGroupUnexpectedProbeErrorFailsCleanup(t *testing.T) {
	sentinel := errors.New("unexpected process-group probe failure")
	if err := cleanupManagedProcessGroupWithProbe(12345, time.Millisecond, func(int, syscall.Signal) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("unexpected initial probe error was ignored: %v", err)
	}
}

func TestManagedProcessGroupMarkerFollowsConfirmedKillAndReap(t *testing.T) {
	stateDir := t.TempDir()
	readyPath := filepath.Join(stateDir, "ready")
	childReadyPath := filepath.Join(stateDir, "child-ready")
	script := fmt.Sprintf("trap '' TERM INT HUP QUIT; (trap '' TERM INT HUP QUIT; printf child-ready > %q; while :; do sleep 1; done) & child=$!; while [ ! -f %q ]; do sleep 0.01; done; printf ready > %q; wait $child", childReadyPath, childReadyPath, readyPath)
	cmd := exec.Command("/bin/sh", "-c", script)
	configureManagedProcess(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan error, 1)
	var waitCalls atomic.Int32
	go func() {
		waitCalls.Add(1)
		waitResult <- cmd.Wait()
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("managed process group did not publish readiness handshake")
		case <-time.After(time.Millisecond):
		}
	}
	var exitObservedBeforeGroupGone bool
	var termIssued, killIssued atomic.Bool
	probe := func(target int, signal syscall.Signal) error {
		if signal == syscall.SIGTERM {
			termIssued.Store(true)
		}
		if signal == syscall.SIGKILL {
			killIssued.Store(true)
		}
		return syscall.Kill(target, signal)
	}
	ops := managedFinalizeOperations{
		writeFile: func(dir, name, contents string) error {
			if name == "exit" {
				if !termIssued.Load() || !killIssued.Load() {
					t.Fatalf("exit marker was published before TERM and SIGKILL: term=%v kill=%v", termIssued.Load(), killIssued.Load())
				}
				if err := syscall.Kill(-cmd.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
					exitObservedBeforeGroupGone = true
				}
				if !strings.HasPrefix(contents, "signal:") {
					t.Fatalf("unexpected exit marker=%q", contents)
				}
			}
			return writeManagedFile(dir, name, contents)
		},
		cleanupProcessGroup: func(pid int, grace time.Duration) error {
			return cleanupManagedProcessGroupWithProbe(pid, grace, probe)
		},
	}
	if code := managedFinalizeStatusWithOperations(stateDir, "signal:terminated", cmd.Process.Pid, ops); code == managedProtocolCode {
		t.Fatalf("confirmed process-group cleanup reported protocol failure")
	}
	if exitObservedBeforeGroupGone {
		t.Fatal("exit marker was observable before process-group disappearance")
	}
	if !termIssued.Load() || !killIssued.Load() {
		t.Fatalf("force-kill cleanup branch was not exercised: term=%v kill=%v", termIssued.Load(), killIssued.Load())
	}
	select {
	case <-waitResult:
	case <-time.After(5 * time.Second):
		t.Fatal("primary child was not reaped")
	}
	if cmd.ProcessState == nil {
		t.Fatal("primary child has no ProcessState after Wait")
	}
	if got := waitCalls.Load(); got != 1 {
		t.Fatalf("primary child was reaped %d times, want exactly once", got)
	}
	if _, err := os.Stat(stateDir + "/exit"); err != nil {
		t.Fatalf("exit marker was not published: %v", err)
	}
}

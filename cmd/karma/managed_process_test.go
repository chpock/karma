package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	managedStartupExitCode = 123
	managedStartErrorCode  = 125
	managedTimeoutCode     = 124
	managedProtocolCode    = 126
)

type managedTimerSource struct {
	C    <-chan time.Time
	Stop func() bool
}

type managedTickerSource struct {
	C    <-chan time.Time
	Stop func()
}

type managedKarmaDependencies struct {
	resolveExecutable func() (string, error)
	command           func(string, []string) *exec.Cmd
	newTimer          func(time.Duration) managedTimerSource
	newTicker         func(time.Duration) managedTickerSource
}

func managedKarmaMain() {
	os.Exit(runManagedKarma(os.Args[1:]))
}

func managedWaitPIDMain() {
	if err := waitManagedReady(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func managedWaitExitMain() {
	if err := waitManagedExit(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func managedCheckResultMain() {
	if err := checkManagedResultFile(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runManagedKarma(args []string) int {
	return runManagedKarmaWithDependencies(args, productionManagedKarmaDependencies())
}

func productionManagedKarmaDependencies() managedKarmaDependencies {
	return managedKarmaDependencies{
		resolveExecutable: func() (string, error) { return exec.LookPath("karma") },
		command: func(executable string, args []string) *exec.Cmd {
			return exec.Command(executable, args...)
		},
		newTimer: func(duration time.Duration) managedTimerSource {
			timer := time.NewTimer(duration)
			return managedTimerSource{C: timer.C, Stop: timer.Stop}
		},
		newTicker: func(duration time.Duration) managedTickerSource {
			ticker := time.NewTicker(duration)
			return managedTickerSource{C: ticker.C, Stop: ticker.Stop}
		},
	}
}

func runManagedKarmaWithDependencies(args []string, deps managedKarmaDependencies) int {
	if len(args) < 6 {
		return managedUsageError("managed-karma requires STATE_DIR PID_FILE STARTUP_DURATION SHUTDOWN_GRACE -- karma [args...]")
	}
	stateDir, pidFile := args[0], args[1]
	startupDuration, err := time.ParseDuration(args[2])
	if err != nil || startupDuration <= 0 {
		return managedUsageError(fmt.Sprintf("invalid startup duration: args=%q", args))
	}
	shutdownGrace, err := time.ParseDuration(args[3])
	if err != nil || shutdownGrace <= 0 {
		return managedUsageError(fmt.Sprintf("invalid shutdown grace: args=%q", args))
	}
	if args[4] != "karma" {
		return managedUsageError("managed-karma must launch the karma test command")
	}
	if err = os.Mkdir(stateDir, 0o700); err != nil {
		return managedUsageError(fmt.Sprintf("create state directory: %v", err))
	}
	if err = writeManagedFile(stateDir, "wrapper-pid", fmt.Sprintf("%d\n", os.Getpid())); err != nil {
		_ = writeManagedMarker(stateDir, "failed:marker:wrapper_pid")
		_ = writeManagedError(stateDir, err)
		return managedProtocolCode
	}

	executable, err := deps.resolveExecutable()
	if err != nil {
		_ = writeManagedMarker(stateDir, "failed:start_error:none")
		_ = writeManagedError(stateDir, err)
		return managedStartErrorCode
	}
	cmd := deps.command(executable, append([]string{"karma"}, args[5:]...))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	configureManagedProcess(cmd)
	if err := cmd.Start(); err != nil {
		_ = writeManagedMarker(stateDir, "failed:start_error:none")
		_ = writeManagedError(stateDir, err)
		return managedStartErrorCode
	}
	waitResult := make(chan error, 1)
	go func() {
		waitResult <- cmd.Wait()
	}()
	if err := writeManagedFile(stateDir, "child-pid", fmt.Sprintf("%d\n", cmd.Process.Pid)); err != nil {
		_, _ = terminateManagedProcess(cmd, waitResult, shutdownGrace)
		_ = writeManagedMarker(stateDir, "failed:marker:child_pid")
		_ = writeManagedError(stateDir, err)
		return managedProtocolCode
	}

	startupTimer := deps.newTimer(startupDuration)
	defer startupTimer.Stop()
	readinessTicker := deps.newTicker(10 * time.Millisecond)
	defer readinessTicker.Stop()
	signalCh := make(chan os.Signal, 8)
	managedNotifySignals(signalCh)
	defer managedStopSignals(signalCh)

	for {
		select {
		case err := <-waitResult:
			status := managedStatusToken(err)
			_ = writeManagedMarker(stateDir, "failed:child_exit:"+status)
			return managedStartupExitCode
		case <-signalCh:
			status, terminateErr := terminateManagedProcess(cmd, waitResult, shutdownGrace)
			if terminateErr != nil {
				_ = writeManagedError(stateDir, terminateErr)
			}
			_ = writeManagedMarker(stateDir, "failed:child_exit:"+status)
			return managedStartupExitCode
		case <-startupTimer.C:
			status, terminateErr := terminateManagedProcess(cmd, waitResult, shutdownGrace)
			if terminateErr != nil {
				_ = writeManagedError(stateDir, terminateErr)
			}
			_ = writeManagedMarker(stateDir, "failed:timeout:"+status)
			_ = writeManagedError(stateDir, fmt.Errorf("startup timeout after %s", startupDuration))
			return managedTimeoutCode
		case <-readinessTicker.C:
			ready, err := managedPIDMatches(pidFile, cmd.Process.Pid)
			if err != nil || !ready {
				continue
			}
			select {
			case err := <-waitResult:
				status := managedStatusToken(err)
				_ = writeManagedMarker(stateDir, "failed:child_exit:"+status)
				return managedStartupExitCode
			default:
			}
			if err := writeManagedMarker(stateDir, fmt.Sprintf("ready:%d", cmd.Process.Pid)); err != nil {
				status, terminateErr := terminateManagedProcess(cmd, waitResult, shutdownGrace)
				if terminateErr != nil {
					_ = writeManagedError(stateDir, terminateErr)
				}
				_ = writeManagedMarker(stateDir, "failed:marker:ready")
				_ = writeManagedError(stateDir, err)
				_ = status
				return managedProtocolCode
			}
			return managedWaitAfterReady(stateDir, waitResult, signalCh, cmd, shutdownGrace)
		}
	}
}

func managedWaitAfterReady(stateDir string, waitResult <-chan error, signalCh <-chan os.Signal, cmd *exec.Cmd, shutdownGrace time.Duration) int {
	for {
		select {
		case err := <-waitResult:
			return managedFinalizeExit(stateDir, err, cmd.Process.Pid)
		case <-signalCh:
			status, terminateErr := terminateManagedProcess(cmd, waitResult, shutdownGrace)
			if terminateErr != nil {
				_ = writeManagedError(stateDir, terminateErr)
				return managedProtocolCode
			}
			return managedFinalizeStatus(stateDir, status, cmd.Process.Pid)
		}
	}
}

func managedFinalizeExit(stateDir string, err error, pid int) int {
	return managedFinalizeStatus(stateDir, managedStatusToken(err), pid)
}

func managedFinalizeStatus(stateDir, status string, pid int) int {
	_ = writeManagedFile(stateDir, "exit", status+"\n")
	if cleanupErr := cleanupManagedProcessGroup(pid, 2*time.Second); cleanupErr != nil {
		_ = writeManagedError(stateDir, cleanupErr)
		return managedProtocolCode
	}
	if strings.HasPrefix(status, "exit:") {
		code, err := strconv.Atoi(strings.TrimPrefix(status, "exit:"))
		if err == nil {
			return code
		}
	}
	return 128
}

func managedUsageError(message string) int {
	_, _ = fmt.Fprintln(os.Stderr, message)
	return managedProtocolCode
}

func managedPIDMatches(path string, expectedPID int) (bool, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	value := strings.TrimSpace(string(contents))
	if value == "" {
		return false, nil
	}
	pid, err := strconv.Atoi(value)
	if err != nil || pid <= 0 {
		return false, nil
	}
	return pid == expectedPID, nil
}

func managedStatusToken(err error) string {
	if err == nil {
		return "exit:0"
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if code := exitErr.ExitCode(); code >= 0 {
			return fmt.Sprintf("exit:%d", code)
		}
		return managedSignalToken(err)
	}
	return "signal:unknown"
}

func writeManagedMarker(stateDir, marker string) error {
	return writeManagedFile(stateDir, "startup", marker+"\n")
}

func writeManagedError(stateDir string, err error) error {
	if err == nil {
		return nil
	}
	return writeManagedFile(stateDir, "harness-error", err.Error()+"\n")
}

func writeManagedFile(stateDir, name, contents string) error {
	temporary, err := os.CreateTemp(stateDir, ".managed-tmp-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if _, err := temporary.WriteString(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(stateDir, name))
}

func waitManagedReady(args []string) error {
	if len(args) != 3 {
		return errors.New("wait-managed-pid requires STATE_DIR STARTUP_DURATION SHUTDOWN_GRACE")
	}
	startupDuration, err := time.ParseDuration(args[1])
	if err != nil {
		return err
	}
	shutdownGrace, err := time.ParseDuration(args[2])
	if err != nil {
		return err
	}
	deadline := time.Now().Add(startupDuration + shutdownGrace + 2*time.Second)
	for time.Now().Before(deadline) {
		contents, readErr := os.ReadFile(filepath.Join(args[0], "startup"))
		if readErr == nil {
			marker := strings.TrimSpace(string(contents))
			if strings.HasPrefix(marker, "ready:") {
				return nil
			}
			return fmt.Errorf("managed-karma startup failed: %s", marker)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("wait-managed-pid timed out for %s", args[0])
}

func waitManagedExit(args []string) error {
	if len(args) != 3 {
		return errors.New("wait-managed-exit requires STATE_DIR STARTUP_DURATION SHUTDOWN_GRACE")
	}
	startupDuration, err := time.ParseDuration(args[1])
	if err != nil {
		return err
	}
	shutdownGrace, err := time.ParseDuration(args[2])
	if err != nil {
		return err
	}
	deadline := time.Now().Add(startupDuration + shutdownGrace + 2*time.Second)
	for time.Now().Before(deadline) {
		contents, readErr := os.ReadFile(filepath.Join(args[0], "exit"))
		if readErr == nil {
			marker := strings.TrimSpace(string(contents))
			if strings.HasPrefix(marker, "exit:") || strings.HasPrefix(marker, "signal:") {
				return nil
			}
			return fmt.Errorf("managed-karma exit marker is malformed: %s", marker)
		}
		if startup, startupErr := os.ReadFile(filepath.Join(args[0], "startup")); startupErr == nil && strings.HasPrefix(strings.TrimSpace(string(startup)), "failed:") {
			return fmt.Errorf("managed-karma exited before readiness: %s", strings.TrimSpace(string(startup)))
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("wait-managed-exit timed out for %s", args[0])
}

func checkManagedResultFile(args []string) error {
	if len(args) != 1 {
		return errors.New("check-managed-result requires STATE_DIR")
	}
	if contents, err := os.ReadFile(filepath.Join(args[0], "harness-error")); err == nil {
		return fmt.Errorf("managed-karma harness error: %s", strings.TrimSpace(string(contents)))
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read managed-karma harness error: %w", err)
	}
	return nil
}

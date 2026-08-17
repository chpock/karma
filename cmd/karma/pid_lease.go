package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrPIDLeaseAlreadyOwned        = errors.New("PID file lease is already owned")
	ErrPIDLeaseOwnershipLost       = errors.New("PID file lease ownership was lost")
	ErrPIDLeaseIdentityUnavailable = errors.New("PID file lease identity is unavailable")
)

// pidFileLease trusts the configured directory but protects against accidental
// overwrite/removal and detects path replacement between acquire and release. A
// privileged actor can still replace the path after the final identity check; that
// narrow race is outside the application's local ownership threat model.
type pidFileLease struct {
	configuredPath string
	resolvedPath   string
	owned          bool
	file           pidLeaseFile
	identity       pidFileIdentity
	identityValid  bool
	ops            *pidFileOps
}

type pidFileOps struct {
	resolvePath                func(string) (string, error)
	create                     func(string) (pidLeaseFile, error)
	captureIdentity            func(*pidFileLease) (pidFileIdentity, error)
	cleanupAfterAcquireFailure func(*pidFileLease) error
	release                    func(*pidFileLease) error
}

func newPIDFileLease(ops *pidFileOps) *pidFileLease {
	if ops == nil {
		ops = &defaultPIDLeaseOps
	}
	return &pidFileLease{ops: ops}
}

func (lease *pidFileLease) operations() *pidFileOps {
	if lease.ops == nil {
		lease.ops = &defaultPIDLeaseOps
	}
	return lease.ops
}

func (lease *pidFileLease) Acquire(configuredPath string, pid int) error {
	if lease.owned {
		return ErrPIDLeaseAlreadyOwned
	}
	configuredPath = strings.TrimSpace(configuredPath)
	if configuredPath == "" {
		return nil
	}
	ops := lease.operations()
	resolvedPath, err := ops.resolvePath(configuredPath)
	if err != nil {
		return fmt.Errorf("resolve PID file path: %w", err)
	}
	file, err := ops.create(resolvedPath)
	if err != nil {
		return fmt.Errorf("create PID file: %w", err)
	}
	lease.configuredPath = configuredPath
	lease.resolvedPath = resolvedPath
	lease.file = file
	lease.owned = true

	identity, identityErr := ops.captureIdentity(lease)
	if identityErr != nil {
		cleanupErr := ops.cleanupAfterAcquireFailure(lease)
		return errors.Join(fmt.Errorf("capture PID file identity: %w", identityErr), cleanupErr)
	}
	lease.identity = identity
	lease.identityValid = true

	if err = pidLeaseWriteFull(file, []byte(strconv.Itoa(pid))); err != nil {
		cleanupErr := ops.release(lease)
		return errors.Join(fmt.Errorf("write PID file: %w", err), cleanupErr)
	}
	if err = file.Sync(); err != nil {
		cleanupErr := ops.release(lease)
		return errors.Join(fmt.Errorf("sync PID file: %w", err), cleanupErr)
	}
	return nil
}

func (lease *pidFileLease) Release() error {
	if !lease.owned {
		return nil
	}
	return lease.operations().release(lease)
}

func (lease *pidFileLease) Owned() bool {
	return lease.owned
}

func pidLeaseWriteFull(file pidLeaseFile, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if written < 0 || written > len(data) {
			return fmt.Errorf("invalid PID file write count %d for %d bytes", written, len(data))
		}
		if written == 0 && err == nil {
			return errors.New("PID file write made no progress")
		}
		data = data[written:]
		if err != nil {
			return err
		}
	}
	return nil
}

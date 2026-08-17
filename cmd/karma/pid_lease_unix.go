//go:build !windows

package main

import (
	"errors"
	"os"
	"path/filepath"
)

type pidLeaseFile interface {
	Write([]byte) (int, error)
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

type pidFileIdentity struct {
	fileInfo os.FileInfo
}

var defaultPIDLeaseOps = pidFileOps{
	resolvePath:                pidLeaseResolvePath,
	create:                     pidLeaseCreate,
	captureIdentity:            pidLeaseCaptureIdentity,
	cleanupAfterAcquireFailure: pidLeaseCleanupAfterAcquireFailure,
	release:                    pidLeaseRelease,
}

func pidLeaseResolvePath(path string) (string, error) {
	return filepath.Abs(path)
}

func pidLeaseCreate(path string) (pidLeaseFile, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
}

func pidLeaseCaptureIdentity(lease *pidFileLease) (pidFileIdentity, error) {
	if lease.file == nil {
		return pidFileIdentity{}, ErrPIDLeaseOwnershipLost
	}
	fileInfo, err := lease.file.Stat()
	if err != nil {
		return pidFileIdentity{}, err
	}
	pathInfo, err := os.Lstat(lease.resolvedPath)
	if err != nil {
		return pidFileIdentity{}, err
	}
	if !os.SameFile(fileInfo, pathInfo) {
		return pidFileIdentity{}, ErrPIDLeaseOwnershipLost
	}
	return pidFileIdentity{fileInfo: fileInfo}, nil
}

func pidLeaseCleanupAfterAcquireFailure(lease *pidFileLease) error {
	if !lease.owned {
		return nil
	}
	identity, err := pidLeaseCaptureIdentity(lease)
	if err == nil {
		lease.identity = identity
		lease.identityValid = true
		return pidLeaseRelease(lease)
	}
	return errors.Join(ErrPIDLeaseIdentityUnavailable, err)
}

func pidLeaseRelease(lease *pidFileLease) error {
	if lease.file == nil {
		return pidLeaseOwnershipLost(lease, ErrPIDLeaseOwnershipLost)
	}
	if !lease.identityValid {
		identity, err := pidLeaseCaptureIdentity(lease)
		if err != nil {
			if errors.Is(err, ErrPIDLeaseOwnershipLost) || errors.Is(err, os.ErrNotExist) {
				return pidLeaseOwnershipLost(lease, err)
			}
			return errors.Join(ErrPIDLeaseIdentityUnavailable, err)
		}
		lease.identity = identity
		lease.identityValid = true
	}
	pathInfo, err := os.Lstat(lease.resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pidLeaseOwnershipLost(lease, err)
		}
		return err
	}
	if !os.SameFile(lease.identity.fileInfo, pathInfo) {
		return pidLeaseOwnershipLost(lease, ErrPIDLeaseOwnershipLost)
	}
	if err = os.Remove(lease.resolvedPath); err != nil {
		return err
	}
	closeErr := lease.file.Close()
	lease.clear()
	return closeErr
}

func pidLeaseOwnershipLost(lease *pidFileLease, _ error) error {
	var closeErr error
	if lease.file != nil {
		closeErr = lease.file.Close()
	}
	lease.clear()
	return errors.Join(ErrPIDLeaseOwnershipLost, closeErr)
}

func (lease *pidFileLease) clear() {
	lease.configuredPath = ""
	lease.resolvedPath = ""
	lease.owned = false
	lease.file = nil
	lease.identity = pidFileIdentity{}
	lease.identityValid = false
}

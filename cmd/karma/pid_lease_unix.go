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
	return pidLeaseReleaseWithPrimitives(lease, pidLeasePlatformReleasePrimitives())
}

func pidLeasePlatformReleasePrimitives() pidLeaseReleasePrimitives {
	return pidLeaseReleasePrimitives{
		captureIdentity: pidLeaseCaptureIdentity,
		captureIdentityOwnershipLost: func(err error) bool {
			return errors.Is(err, ErrPIDLeaseOwnershipLost) || errors.Is(err, os.ErrNotExist)
		},
		openCurrent: func(lease *pidFileLease) pidLeaseCurrentResult {
			pathInfo, err := os.Lstat(lease.resolvedPath)
			if err != nil {
				return pidLeaseCurrentResult{err: err, ownershipLost: errors.Is(err, os.ErrNotExist)}
			}
			return pidLeaseCurrentResult{handle: &pidLeaseCurrentHandle{
				identity: pidFileIdentity{fileInfo: pathInfo},
				close:    func() error { return nil },
				delete:   func() error { return os.Remove(lease.resolvedPath) },
			}}
		},
		sameIdentity: func(expected, current pidFileIdentity) bool {
			return os.SameFile(expected.fileInfo, current.fileInfo)
		},
	}
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

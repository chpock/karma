//go:build windows

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

type pidLeaseFile interface {
	Write([]byte) (int, error)
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

type pidFileIdentity struct {
	volumeSerial  uint32
	fileIndexHigh uint32
	fileIndexLow  uint32
}

var errPIDLeaseReparsePoint = errors.New("PID file is a reparse point")

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
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(path),
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

func pidLeaseCaptureIdentity(lease *pidFileLease) (pidFileIdentity, error) {
	if lease.file == nil {
		return pidFileIdentity{}, ErrPIDLeaseOwnershipLost
	}
	return pidLeaseIdentityFromHandle(lease.file)
}

func pidLeaseIdentityFromHandle(file pidLeaseFile) (pidFileIdentity, error) {
	native, ok := file.(*os.File)
	if !ok {
		return pidFileIdentity{}, errors.New("PID file handle is not an OS file")
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(native.Fd()), &info); err != nil {
		return pidFileIdentity{}, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return pidFileIdentity{}, errPIDLeaseReparsePoint
	}
	return pidFileIdentity{volumeSerial: info.VolumeSerialNumber, fileIndexHigh: info.FileIndexHigh, fileIndexLow: info.FileIndexLow}, nil
}

func pidLeaseCleanupAfterAcquireFailure(lease *pidFileLease) error {
	identity, err := pidLeaseCaptureIdentity(lease)
	if err != nil {
		return errors.Join(ErrPIDLeaseIdentityUnavailable, err)
	}
	lease.identity = identity
	lease.identityValid = true
	return pidLeaseRelease(lease)
}

func pidLeaseRelease(lease *pidFileLease) error {
	return pidLeaseReleaseWithPrimitives(lease, pidLeasePlatformReleasePrimitives())
}

func pidLeasePlatformReleasePrimitives() pidLeaseReleasePrimitives {
	return pidLeaseReleasePrimitives{
		captureIdentity: pidLeaseCaptureIdentity,
		captureIdentityOwnershipLost: func(err error) bool {
			return errors.Is(err, errPIDLeaseReparsePoint) ||
				errors.Is(err, ErrPIDLeaseOwnershipLost)
		},
		openCurrent: func(lease *pidFileLease) pidLeaseCurrentResult {
			current, err := windows.CreateFile(
				windows.StringToUTF16Ptr(lease.resolvedPath),
				windows.FILE_READ_ATTRIBUTES,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
				nil,
				windows.OPEN_EXISTING,
				windows.FILE_FLAG_OPEN_REPARSE_POINT,
				0,
			)
			if err != nil {
				return pidLeaseCurrentResult{
					err:           err,
					ownershipLost: errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND),
				}
			}
			currentFile := os.NewFile(uintptr(current), lease.resolvedPath)
			currentHandle := &pidLeaseCurrentHandle{
				close: currentFile.Close,
				delete: func() error {
					if err := windows.DeleteFile(windows.StringToUTF16Ptr(lease.resolvedPath)); err != nil {
						return fmt.Errorf("delete PID file: %w", err)
					}
					return nil
				},
			}
			identity, identityErr := pidLeaseIdentityFromHandle(currentFile)
			if identityErr != nil {
				return pidLeaseCurrentResult{handle: currentHandle, err: identityErr, ownershipLost: errors.Is(identityErr, errPIDLeaseReparsePoint)}
			}
			currentHandle.identity = identity
			return pidLeaseCurrentResult{handle: currentHandle}
		},
		sameIdentity: func(expected, current pidFileIdentity) bool {
			return expected == current
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

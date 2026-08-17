package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPIDLeaseRejectsExistingPathWithoutChangingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	if err := os.WriteFile(path, []byte("old-pid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 123); !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing PID acquire error: got=%v want os.ErrExist", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "old-pid\n" {
		t.Fatalf("existing PID content changed: %q", content)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("existing PID mode changed: %o", info.Mode().Perm())
	}
}

func TestStartupCleanupDoesNotRemovePIDFileBeforeThisProcessAcquiresIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	if err := os.WriteFile(path, []byte("previous-process\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := newPIDFileLease(nil)
	cleanup := startupCleanup{releasePID: lease.Release}
	if err := cleanup.run(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "previous-process\n" {
		t.Fatalf("startup cleanup changed an unowned PID file: %q", content)
	}
}

func TestPIDLeaseDoesNotFollowSymlinkOrDirectory(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.pid")
	link := filepath.Join(dir, "karma.pid")
	if err := os.WriteFile(target, []byte("target\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(link, 123); !errors.Is(err, os.ErrExist) {
		t.Fatalf("symlink PID acquire error: got=%v want os.ErrExist", err)
	}
	content, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "target\n" {
		t.Fatalf("symlink target changed: %q", content)
	}

	directory := filepath.Join(dir, "directory.pid")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := lease.Acquire(directory, 123); !errors.Is(err, os.ErrExist) {
		t.Fatalf("directory PID acquire error: got=%v want os.ErrExist", err)
	}
}

func TestPIDLeaseAcquireWriteRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 12345); err != nil {
		t.Fatal(err)
	}
	if !lease.Owned() {
		t.Fatal("lease is not owned after exclusive acquire")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "12345" {
		t.Fatalf("PID content: got=%q want=12345", content)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	if lease.Owned() {
		t.Fatal("lease remains owned after release")
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("PID path after release: %v", err)
	}
}

func TestPIDLeaseSecondAcquireDoesNotChangeOriginalLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	first := newPIDFileLease(nil)
	if err := first.Acquire(path, 1); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Release() }()
	originalPath := first.resolvedPath
	if err := first.Acquire("", 2); !errors.Is(err, ErrPIDLeaseAlreadyOwned) {
		t.Fatalf("second acquire error: got=%v", err)
	}
	if first.resolvedPath != originalPath || !first.Owned() {
		t.Fatal("second acquire changed the original lease")
	}
}

func TestPIDLeaseReleaseWithoutOwnershipIsNoop(t *testing.T) {
	lease := newPIDFileLease(nil)
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

type fakePIDLeaseFile struct {
	data       []byte
	writeLimit int
	writeErr   error
	syncErr    error
	closeErr   error
	statInfo   os.FileInfo
	statErr    error
}

func (f *fakePIDLeaseFile) Write(data []byte) (int, error) {
	if f.writeLimit == 0 {
		return 0, f.writeErr
	}
	written := len(data)
	if f.writeLimit > 0 && written > f.writeLimit {
		written = f.writeLimit
	}
	f.data = append(f.data, data[:written]...)
	if f.writeErr != nil {
		return written, f.writeErr
	}
	return written, nil
}

func (f *fakePIDLeaseFile) Sync() error { return f.syncErr }

func (f *fakePIDLeaseFile) Stat() (os.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	if f.statInfo != nil {
		return f.statInfo, nil
	}
	return nil, errors.New("fake stat unused")
}

func (f *fakePIDLeaseFile) Close() error { return f.closeErr }

func TestPIDLeasePartialWriteRunsOwnedCleanup(t *testing.T) {
	writeErr := errors.New("partial write")
	cleanupErr := errors.New("cleanup failed")
	fakeFile := &fakePIDLeaseFile{writeLimit: 1, writeErr: writeErr}
	releaseCalls := 0
	ops := &pidFileOps{
		resolvePath:                func(path string) (string, error) { return path, nil },
		create:                     func(string) (pidLeaseFile, error) { return fakeFile, nil },
		captureIdentity:            func(*pidFileLease) (pidFileIdentity, error) { return pidFileIdentity{}, nil },
		cleanupAfterAcquireFailure: func(*pidFileLease) error { return nil },
		release: func(_ *pidFileLease) error {
			releaseCalls++
			return cleanupErr
		},
	}
	lease := newPIDFileLease(ops)
	err := lease.Acquire("fake.pid", 12345)
	if !errors.Is(err, writeErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("partial write errors: %v", err)
	}
	if releaseCalls != 1 || !lease.Owned() {
		t.Fatalf("partial write cleanup: calls=%d owned=%t", releaseCalls, lease.Owned())
	}
	if string(fakeFile.data) != "1" {
		t.Fatalf("partial write data: %q", fakeFile.data)
	}
}

func TestPIDLeaseIdentityFailureRetainsOwnershipWhenCleanupCannotProvePath(t *testing.T) {
	identityErr := errors.New("identity unavailable")
	cleanupErr := errors.New("cleanup identity unavailable")
	fakeFile := &fakePIDLeaseFile{writeLimit: -1}
	ops := &pidFileOps{
		resolvePath:                func(path string) (string, error) { return path, nil },
		create:                     func(string) (pidLeaseFile, error) { return fakeFile, nil },
		captureIdentity:            func(*pidFileLease) (pidFileIdentity, error) { return pidFileIdentity{}, identityErr },
		cleanupAfterAcquireFailure: func(*pidFileLease) error { return cleanupErr },
		release:                    func(*pidFileLease) error { return nil },
	}
	lease := newPIDFileLease(ops)
	err := lease.Acquire("fake.pid", 12345)
	if !errors.Is(err, identityErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("identity cleanup errors: %v", err)
	}
	if !lease.Owned() {
		t.Fatal("lease ownership was lost after unproven cleanup")
	}
}

func TestPIDLeaseIdentityFailureCleansUpAfterRetrySucceeds(t *testing.T) {
	identityErr := errors.New("initial identity failure")
	fakeFile := &fakePIDLeaseFile{writeLimit: -1}
	captureCalls := 0
	releaseCalls := 0
	ops := &pidFileOps{
		resolvePath: func(path string) (string, error) { return path, nil },
		create:      func(string) (pidLeaseFile, error) { return fakeFile, nil },
		captureIdentity: func(*pidFileLease) (pidFileIdentity, error) {
			captureCalls++
			if captureCalls == 1 {
				return pidFileIdentity{}, identityErr
			}
			return pidFileIdentity{}, nil
		},
		cleanupAfterAcquireFailure: func(lease *pidFileLease) error {
			identity, err := lease.ops.captureIdentity(lease)
			if err != nil {
				return err
			}
			lease.identity = identity
			lease.identityValid = true
			return lease.ops.release(lease)
		},
		release: func(lease *pidFileLease) error {
			releaseCalls++
			lease.clear()
			return nil
		},
	}
	lease := newPIDFileLease(ops)
	err := lease.Acquire("fake.pid", 12345)
	if !errors.Is(err, identityErr) {
		t.Fatalf("identity error: %v", err)
	}
	if lease.Owned() || captureCalls != 2 || releaseCalls != 1 {
		t.Fatalf("identity cleanup: owned=%t captures=%d releases=%d", lease.Owned(), captureCalls, releaseCalls)
	}
}

func TestPIDLeaseSuccessfulRemovalClearsStateEvenWhenCloseFails(t *testing.T) {
	closeErr := errors.New("close failed after removal")
	fakeFile := &fakePIDLeaseFile{writeLimit: -1, closeErr: closeErr}
	ops := &pidFileOps{
		resolvePath:                func(path string) (string, error) { return path, nil },
		create:                     func(string) (pidLeaseFile, error) { return fakeFile, nil },
		captureIdentity:            func(*pidFileLease) (pidFileIdentity, error) { return pidFileIdentity{}, nil },
		cleanupAfterAcquireFailure: func(*pidFileLease) error { return nil },
		release: func(lease *pidFileLease) error {
			lease.clear()
			return closeErr
		},
	}
	lease := newPIDFileLease(ops)
	if err := lease.Acquire("fake.pid", 12345); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); !errors.Is(err, closeErr) {
		t.Fatalf("release close error: %v", err)
	}
	if lease.Owned() {
		t.Fatal("lease remained owned after successful removal and close failure")
	}
}

func TestPIDLeaseFailedAcquireCleanupJoinsCloseError(t *testing.T) {
	identityErr := errors.New("initial stat failed")
	closeErr := errors.New("cleanup close failed")
	fakeFile := &fakePIDLeaseFile{writeLimit: -1}
	captureCalls := 0
	ops := &pidFileOps{
		resolvePath: func(path string) (string, error) { return path, nil },
		create:      func(string) (pidLeaseFile, error) { return fakeFile, nil },
		captureIdentity: func(*pidFileLease) (pidFileIdentity, error) {
			captureCalls++
			if captureCalls == 1 {
				return pidFileIdentity{}, identityErr
			}
			return pidFileIdentity{}, nil
		},
		cleanupAfterAcquireFailure: func(lease *pidFileLease) error {
			identity, err := lease.ops.captureIdentity(lease)
			if err != nil {
				return err
			}
			lease.identity = identity
			lease.identityValid = true
			lease.clear()
			return closeErr
		},
		release: func(*pidFileLease) error { return nil },
	}
	lease := newPIDFileLease(ops)
	err := lease.Acquire("fake.pid", 12345)
	if !errors.Is(err, identityErr) || !errors.Is(err, closeErr) {
		t.Fatalf("failed-acquire cleanup errors: %v", err)
	}
	if lease.Owned() {
		t.Fatal("lease remained owned after successful cleanup")
	}
}

func TestPIDLeaseFailedReleaseRetainsStateForRetry(t *testing.T) {
	lstatErr := errors.New("path stat failed")
	fakeFile := &fakePIDLeaseFile{writeLimit: -1}
	ops := &pidFileOps{
		resolvePath:                func(path string) (string, error) { return path, nil },
		create:                     func(string) (pidLeaseFile, error) { return fakeFile, nil },
		captureIdentity:            func(*pidFileLease) (pidFileIdentity, error) { return pidFileIdentity{}, nil },
		cleanupAfterAcquireFailure: func(*pidFileLease) error { return nil },
		release:                    func(*pidFileLease) error { return lstatErr },
	}
	lease := newPIDFileLease(ops)
	if err := lease.Acquire("fake.pid", 12345); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); !errors.Is(err, lstatErr) {
		t.Fatalf("failed release error: %v", err)
	}
	if !lease.Owned() {
		t.Fatal("lease did not retain state after non-removal failure")
	}
}

func TestPIDLeaseReleaseRecapturesInvalidIdentityThroughProductionPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses Unix path identity primitives")
	}
	path := filepath.Join(t.TempDir(), "karma.pid")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	pathInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	fakeFile := &fakePIDLeaseFile{statInfo: pathInfo}
	lease := &pidFileLease{
		configuredPath: path,
		resolvedPath:   path,
		owned:          true,
		file:           fakeFile,
		ops:            &defaultPIDLeaseOps,
	}
	if err := pidLeaseRelease(lease); err != nil {
		t.Fatal(err)
	}
	if lease.Owned() {
		t.Fatal("lease remained owned after identity recapture and release")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("PID path after recaptured release: %v", err)
	}
}

func TestPIDLeaseInvalidIdentityTransientRecaptureRetainsOwnership(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses Unix path identity primitives")
	}
	identityErr := errors.New("temporary identity failure")
	path := filepath.Join(t.TempDir(), "karma.pid")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := &pidFileLease{
		configuredPath: path,
		resolvedPath:   path,
		owned:          true,
		file:           &fakePIDLeaseFile{statErr: identityErr},
		ops:            &defaultPIDLeaseOps,
	}
	if err := pidLeaseRelease(lease); !errors.Is(err, ErrPIDLeaseIdentityUnavailable) || !errors.Is(err, identityErr) {
		t.Fatalf("transient recapture error: %v", err)
	}
	if !lease.Owned() {
		t.Fatal("lease ownership was lost after an unproven identity failure")
	}
}

func TestPIDLeaseClosedHandleIsTerminalOwnershipLoss(t *testing.T) {
	lease := &pidFileLease{owned: true, ops: &defaultPIDLeaseOps}
	if err := pidLeaseRelease(lease); !errors.Is(err, ErrPIDLeaseOwnershipLost) {
		t.Fatalf("closed-handle release error: %v", err)
	}
	if lease.Owned() {
		t.Fatal("closed-handle release retained ownership")
	}
}

func TestPIDLeaseConcurrentAcquiresHaveOneOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	type result struct {
		lease *pidFileLease
		err   error
	}
	results := make(chan result, 2)
	for pid := 1; pid <= 2; pid++ {
		go func(pid int) {
			lease := newPIDFileLease(nil)
			err := lease.Acquire(path, pid)
			results <- result{lease: lease, err: err}
		}(pid)
	}
	first := <-results
	second := <-results
	owners := 0
	for _, current := range []result{first, second} {
		if current.err == nil {
			owners++
			if err := current.lease.Release(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if owners != 1 {
		t.Fatalf("concurrent PID lease owners: got=%d want=1", owners)
	}
}

func TestPIDLeaseReleaseDetectsReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix replacement semantics are not available on Windows")
	}
	path := filepath.Join(t.TempDir(), "karma.pid")
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := lease.Release()
	if !errors.Is(err, ErrPIDLeaseOwnershipLost) {
		t.Fatalf("replacement release error: %v", err)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.HasPrefix(string(content), "replacement") {
		t.Fatalf("replacement file changed: %q", content)
	}
}

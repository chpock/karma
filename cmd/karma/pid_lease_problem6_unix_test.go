//go:build !windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type transientPIDLeaseFile struct {
	info        os.FileInfo
	identityErr error
	statCalls   int
	closeErr    error
}

func (f *transientPIDLeaseFile) Write(data []byte) (int, error) { return len(data), nil }

func (f *transientPIDLeaseFile) Sync() error { return nil }

func (f *transientPIDLeaseFile) Stat() (os.FileInfo, error) {
	f.statCalls++
	if f.statCalls == 1 {
		return nil, f.identityErr
	}
	return f.info, nil
}

func (f *transientPIDLeaseFile) Close() error { return f.closeErr }

func TestPIDLeaseUnixReleaseRetriesTransientIdentityCapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	identityErr := errors.New("temporary identity capture failure")
	file := &transientPIDLeaseFile{info: info, identityErr: identityErr}
	lease := &pidFileLease{
		resolvedPath: path,
		owned:        true,
		file:         file,
		identity:     pidFileIdentity{fileInfo: info},
	}

	if err := pidLeaseRelease(lease); !errors.Is(err, ErrPIDLeaseIdentityUnavailable) || !errors.Is(err, identityErr) {
		t.Fatalf("first release error: %v", err)
	}
	if !lease.Owned() {
		t.Fatal("transient identity failure lost ownership")
	}
	if err := pidLeaseRelease(lease); err != nil {
		t.Fatalf("retry release error: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), problem6LeaseSnapshot{})
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("PID path after retry release: %v", err)
	}
	if file.statCalls != 2 {
		t.Fatalf("identity capture calls: got=%d want=2", file.statCalls)
	}
}

func TestPIDLeaseUnixReleaseJoinsOwnedCloseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("owned handle close failed")
	lease := &pidFileLease{
		resolvedPath:  path,
		owned:         true,
		file:          &fakePIDLeaseFile{statInfo: info, closeErr: closeErr},
		identity:      pidFileIdentity{fileInfo: info},
		identityValid: true,
	}

	if err := pidLeaseRelease(lease); !errors.Is(err, closeErr) {
		t.Fatalf("release close error: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), problem6LeaseSnapshot{})
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("PID path after close error: %v", err)
	}
}

func TestPIDLeaseUnixOwnershipLossJoinsOwnedCloseError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "karma.pid")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if removeErr := os.Remove(path); removeErr != nil {
		t.Fatal(removeErr)
	}
	if writeErr := os.WriteFile(path, []byte("replacement"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	closeErr := errors.New("owned handle close failed after replacement")
	lease := &pidFileLease{
		resolvedPath:  path,
		owned:         true,
		file:          &fakePIDLeaseFile{statInfo: info, closeErr: closeErr},
		identity:      pidFileIdentity{fileInfo: info},
		identityValid: true,
	}

	err = pidLeaseRelease(lease)
	if !errors.Is(err, ErrPIDLeaseOwnershipLost) || !errors.Is(err, closeErr) {
		t.Fatalf("replacement release errors: %v", err)
	}
	assertProblem6LeaseSnapshotEqual(t, snapshotProblem6Lease(lease), problem6LeaseSnapshot{})
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "replacement" {
		t.Fatalf("replacement path changed: %q", content)
	}
}

//go:build windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPIDLeaseWindowsDeletesOwnedFileWhileHandleIsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 123); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if lease.Owned() {
		t.Fatal("Windows PID lease remains owned after release")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Windows PID path after release: %v", err)
	}
}

func TestPIDLeaseWindowsDetectsReplacementByNativeIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "karma.pid")
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 123); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); !errors.Is(err, ErrPIDLeaseOwnershipLost) {
		t.Fatalf("replacement release error: %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "replacement\n" {
		t.Fatalf("replacement path changed: %q", content)
	}
}

func TestPIDLeaseWindowsDetectsReparseReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "karma.pid")
	target := filepath.Join(dir, "target.pid")
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 123); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("target\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("Windows symlink unavailable: %v", err)
	}
	if err := lease.Release(); !errors.Is(err, ErrPIDLeaseOwnershipLost) {
		t.Fatalf("reparse replacement release error: %v", err)
	}
	content, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "target\n" {
		t.Fatalf("reparse target changed: %q", content)
	}
}

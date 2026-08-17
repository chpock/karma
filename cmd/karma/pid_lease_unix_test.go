//go:build !windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPIDLeaseUnixDetectsParentSymlinkRetarget(t *testing.T) {
	root := t.TempDir()
	firstDir := filepath.Join(root, "first")
	secondDir := filepath.Join(root, "second")
	linkDir := filepath.Join(root, "current")
	for _, dir := range []string{firstDir, secondDir} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(linkDir, "karma.pid")
	if err := os.Symlink(firstDir, linkDir); err != nil {
		t.Fatal(err)
	}
	lease := newPIDFileLease(nil)
	if err := lease.Acquire(path, 123); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(linkDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secondDir, linkDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secondDir, "karma.pid"), []byte("replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); !errors.Is(err, ErrPIDLeaseOwnershipLost) {
		t.Fatalf("parent retarget release error: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(secondDir, "karma.pid"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "replacement\n" {
		t.Fatalf("retargeted replacement changed: %q", content)
	}
}

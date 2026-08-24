//go:build !windows

package main

import (
	"os"
	"time"
)

type problem6SyntheticFileInfo struct{}

func (problem6SyntheticFileInfo) Name() string       { return "problem6.pid" }
func (problem6SyntheticFileInfo) Size() int64        { return 1 }
func (problem6SyntheticFileInfo) Mode() os.FileMode  { return 0o600 }
func (problem6SyntheticFileInfo) ModTime() time.Time { return time.Unix(1, 0) }
func (problem6SyntheticFileInfo) IsDir() bool        { return false }
func (problem6SyntheticFileInfo) Sys() any           { return struct{ device, inode uint64 }{1, 2} }

func problem6NonZeroIdentity() pidFileIdentity {
	return pidFileIdentity{fileInfo: problem6SyntheticFileInfo{}}
}

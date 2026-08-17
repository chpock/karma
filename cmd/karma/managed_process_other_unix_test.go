//go:build !windows && !linux

package main

import "os/exec"

func managedSetParentDeathSignal(_ *exec.Cmd) {}

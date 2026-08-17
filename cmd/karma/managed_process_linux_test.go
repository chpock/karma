//go:build linux

package main

import (
	"os/exec"
	"syscall"
)

func managedSetParentDeathSignal(cmd *exec.Cmd) {
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
}

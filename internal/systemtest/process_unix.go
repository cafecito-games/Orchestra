//go:build system && !windows

package systemtest

import (
	"os/exec"
	"syscall"
	"time"
)

func configureManagedProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateManagedProcess(command *exec.Cmd, exited <-chan error) {
	if command == nil || command.Process == nil {
		return
	}
	select {
	case <-exited:
		return
	default:
	}
	processID := command.Process.Pid
	if processID <= 0 {
		return
	}
	_ = syscall.Kill(-processID, syscall.SIGTERM)
	select {
	case <-exited:
		return
	case <-time.After(3 * time.Second):
		_ = syscall.Kill(-processID, syscall.SIGKILL)
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
		}
	}
}

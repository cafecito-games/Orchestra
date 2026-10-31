//go:build system && windows

package systemtest

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func configureManagedProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func terminateManagedProcess(command *exec.Cmd, exited <-chan error) {
	if command == nil || command.Process == nil || command.Process.Pid <= 0 {
		return
	}
	select {
	case <-exited:
		return
	default:
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	killer := exec.CommandContext(shutdownContext, "taskkill", "/T", "/F", "/PID", strconv.Itoa(command.Process.Pid))
	if err := killer.Run(); err != nil {
		_ = command.Process.Kill()
	}
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
	}
}

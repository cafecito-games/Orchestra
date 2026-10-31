//go:build windows

package antigravityacp

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func configureProcessCancellation(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		killer := exec.CommandContext(context.Background(), "taskkill", "/T", "/F", "/PID", strconv.Itoa(command.Process.Pid))
		err := killer.Run()
		if err != nil && command.ProcessState != nil && command.ProcessState.Exited() {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 2 * time.Second
}

func interruptProcessGroup(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Kill()
}

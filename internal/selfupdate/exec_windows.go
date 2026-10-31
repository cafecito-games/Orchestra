//go:build windows

package selfupdate

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
)

func ExecReplacement(executablePath string, arguments []string, environment []string) error {
	command := exec.CommandContext(context.Background(), executablePath, arguments...)
	command.Env = environment
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return err
	}
	os.Exit(0)
	return nil
}

func isPermissionOrReadOnlyError(err error) bool {
	return errors.Is(err, fs.ErrPermission)
}

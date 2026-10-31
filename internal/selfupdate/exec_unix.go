//go:build !windows

package selfupdate

import (
	"errors"
	"io/fs"
	"syscall"
)

func ExecReplacement(executablePath string, arguments []string, environment []string) error {
	argv := make([]string, 0, len(arguments)+1)
	argv = append(argv, executablePath)
	argv = append(argv, arguments...)
	return syscall.Exec(executablePath, argv, environment)
}

func isPermissionOrReadOnlyError(err error) bool {
	return errors.Is(err, fs.ErrPermission) ||
		errors.Is(err, syscall.EACCES) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EROFS)
}

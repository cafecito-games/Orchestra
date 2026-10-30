package setup

import (
	"os"
	"path/filepath"
)

// DefaultDataRoot returns the Orchestra-owned data root used when no --data-root is given.
func DefaultDataRoot() string {
	base, err := os.UserConfigDir()
	if err != nil {
		base, _ = os.UserHomeDir()
	}
	return filepath.Join(base, "orchestra")
}

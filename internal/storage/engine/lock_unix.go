//go:build unix

package engine

import (
	"fmt"
	"os"
	"syscall"
)

// lockDir takes an exclusive advisory lock so two processes cannot open the
// same database directory.
func lockDir(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("database is locked by another process: %w", err)
	}
	return f, nil
}

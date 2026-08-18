//go:build linux

package dofs

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type stateDirectoryLock struct {
	file *os.File
}

func acquireStateDirectoryLock(stateDirectory string) (*stateDirectoryLock, error) {
	lockPath := filepath.Join(stateDirectory, ".lock")
	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("open DOFS state lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), lockPath)
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("DOFS state directory is already in use: %w", err)
	}
	return &stateDirectoryLock{file: file}, nil
}

func (l *stateDirectoryLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	if err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN); err != nil {
		_ = l.file.Close()
		return err
	}
	err := l.file.Close()
	l.file = nil
	return err
}

//go:build !linux

package dofs

import "errors"

type stateDirectoryLock struct{}

func acquireStateDirectoryLock(string) (*stateDirectoryLock, error) {
	return nil, errors.New("DOFS writeback state is supported on Linux only")
}

func (*stateDirectoryLock) Close() error { return nil }

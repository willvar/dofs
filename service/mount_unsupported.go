//go:build !linux

package service

import (
	"context"
	"errors"

	"github.com/willvar/dofs/config"
)

type MountRequest struct {
	NamespaceID string
	Mountpoint  string
	Writable    bool
	Debug       bool
}

func MountNamespace(context.Context, config.Config, *Runtime, MountRequest) error {
	return errors.New("DOFS FUSE mounts are supported on Linux only")
}

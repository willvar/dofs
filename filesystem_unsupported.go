//go:build !linux

package dofs

import "errors"

func Mount(string, *Backend, MountOptions) (MountedFilesystem, error) {
	return nil, errors.New("DOFS FUSE mounts are supported on Linux only")
}

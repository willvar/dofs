//go:build linux

package dofs_test

import (
	"os"

	"github.com/willvar/dofs"
)

func dofsMountForTest(mountpoint string, backend *dofs.Backend) (dofs.MountedFilesystem, error) {
	return dofs.Mount(mountpoint, backend, dofs.MountOptions{
		Writable: true, Debug: os.Getenv("DOFS_FUSE_DEBUG") == "1",
		UID: uint32(os.Getuid()), GID: uint32(os.Getgid()),
	})
}

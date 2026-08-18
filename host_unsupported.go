//go:build !linux

package dofs

import "errors"

func ValidateHostRequirements(bool) error {
	return errors.New("DOFS FUSE mounts are supported on Linux only")
}

//go:build linux

package dofs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// ValidateHostRequirements turns common FUSE deployment mistakes into a
// startup error before a mount or control service is exposed.
func ValidateHostRequirements(allowOther bool) error {
	device, err := os.OpenFile("/dev/fuse", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open /dev/fuse: %w", err)
	}
	if err := device.Close(); err != nil {
		return fmt.Errorf("close /dev/fuse: %w", err)
	}
	if os.Geteuid() != 0 {
		if _, err := exec.LookPath("fusermount3"); err != nil {
			if _, legacyErr := exec.LookPath("fusermount"); legacyErr != nil {
				return errors.New("fusermount3 is required for an unprivileged DOFS process")
			}
		}
		if allowOther {
			file, err := os.Open("/etc/fuse.conf")
			if err != nil {
				return fmt.Errorf("open /etc/fuse.conf for allow_other: %w", err)
			}
			allowed, readErr := fuseConfigAllowsOther(file)
			closeErr := file.Close()
			if readErr != nil {
				return fmt.Errorf("read /etc/fuse.conf: %w", readErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close /etc/fuse.conf: %w", closeErr)
			}
			if !allowed {
				return errors.New("allow_other requires an uncommented user_allow_other line in /etc/fuse.conf")
			}
		}
	}
	return nil
}

func fuseConfigAllowsOther(reader io.Reader) (bool, error) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(strings.SplitN(scanner.Text(), "#", 2)[0])
		if line == "user_allow_other" {
			return true, nil
		}
	}
	return false, scanner.Err()
}

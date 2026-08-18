//go:build linux

package dofs_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/willvar/dofs"
)

// TestFUSEBindMountIntoDocker proves the intended application boundary: a
// container receives one ordinary plaintext bind mount, while /dev/fuse,
// object credentials and encryption keys remain in the host DOFS process.
func TestFUSEBindMountIntoDocker(t *testing.T) {
	if os.Getenv("DOFS_DOCKER_INTEGRATION") != "1" {
		t.Skip("set DOFS_DOCKER_INTEGRATION=1 to exercise a real Docker bind mount")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI is unavailable")
	}
	if err := dofs.ValidateHostRequirements(true); err != nil {
		t.Skipf("host is not configured for Docker-visible FUSE mounts: %v", err)
	}
	image := os.Getenv("DOFS_DOCKER_IMAGE")
	if image == "" {
		image = "domus-workspace:0.1.0"
	}
	if output, err := exec.Command("docker", "image", "inspect", image).CombinedOutput(); err != nil {
		t.Skipf("container image %q is not present (test never pulls): %v: %s", image, err, output)
	}

	backend, _, _ := newBackend(t, true)
	if err := backend.EnableWriteback(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := dofs.Mount(mountpoint, backend, dofs.MountOptions{
		UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Writable: true, AllowOther: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = mounted.Unmount()
		mounted.Wait()
	}()
	home := filepath.Join(mountpoint, "home", "alice")
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	hostPath := filepath.Join(home, "data.bin")
	if err := os.WriteFile(hostPath, []byte("before-container"), 0600); err != nil {
		t.Fatal(err)
	}

	want := "written-inside-user-container"
	mountArgument := fmt.Sprintf("type=bind,src=%s,dst=/workspace", mountpoint)
	identity := strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
	command := exec.Command(
		"docker", "run", "--rm", "--network=none", "--read-only",
		"--cap-drop=ALL", "--security-opt=no-new-privileges", "--user", identity,
		"--mount", mountArgument, image, "sh", "-c",
		"printf '%s' \"$1\" > \"$2\" && cat \"$2\"", "sh", want, "/workspace/home/alice/data.bin",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run isolated Docker bind-mount probe: %v: %s", err, output)
	}
	if string(output) != want {
		t.Fatalf("container readback = %q, want %q", output, want)
	}
	if data, err := os.ReadFile(hostPath); err != nil || string(data) != want {
		t.Fatalf("host read after container write = %q, err=%v", data, err)
	}
}

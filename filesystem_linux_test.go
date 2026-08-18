//go:build linux

package dofs_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRealFUSEMarvoAtomicIOContract(t *testing.T) {
	if os.Getenv("DOFS_FUSE_INTEGRATION") != "1" {
		t.Skip("set DOFS_FUSE_INTEGRATION=1 to exercise the kernel FUSE contract")
	}
	backend, _, _ := newBackend(t, true)
	stateDirectory := t.TempDir()
	if err := backend.EnableWriteback(t.Context(), stateDirectory); err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := dofsMountForTest(mountpoint, backend)
	if err != nil {
		t.Fatal(err)
	}
	unmounted := false
	defer func() {
		if !unmounted {
			_ = mounted.Unmount()
		}
		mounted.Wait()
	}()

	noteDirectory := filepath.Join(mountpoint, "note")
	if err := os.Mkdir(noteDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	temporary, err := os.CreateTemp(noteDirectory, ".marvo-write-*")
	if err != nil {
		t.Fatal(err)
	}
	temporaryPath := temporary.Name()
	content := []byte("# ordinary filesystem I/O\n\nMarvo does not know about OSS.\n")
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		t.Fatalf("chmod compatibility: %v", err)
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		t.Fatal(err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		t.Fatal(err)
	}
	if err := temporary.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(noteDirectory, "index.md")
	if err := os.Rename(temporaryPath, target); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Fatalf("content = %q", got)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode = %04o", info.Mode().Perm())
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(noteDirectory); err != nil {
		t.Fatal(err)
	}
	if err := mounted.Unmount(); err != nil {
		t.Fatal(err)
	}
	unmounted = true
}

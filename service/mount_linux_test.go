//go:build linux

package service

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareMountpointConstrainsAndProtectsPath(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "mounts")
	mountpoint, err := prepareMountpoint(root, "tenant", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if mountpoint != filepath.Join(root, "tenant") {
		t.Fatalf("mountpoint = %q", mountpoint)
	}
	for _, path := range []string{root, mountpoint} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0700 {
			t.Fatalf("mode of %s = %o", path, info.Mode().Perm())
		}
	}
	if _, err := prepareMountpoint(root, "tenant", filepath.Join(base, "outside"), false); err == nil {
		t.Fatal("mountpoint outside root was accepted")
	}
	if err := os.WriteFile(filepath.Join(mountpoint, "unexpected"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareMountpoint(root, "tenant", "", false); err == nil {
		t.Fatal("non-empty mountpoint was accepted")
	}
}

func TestPrepareMountpointRejectsSymlinkComponents(t *testing.T) {
	base := t.TempDir()
	realRoot := filepath.Join(base, "real")
	if err := os.Mkdir(realRoot, 0700); err != nil {
		t.Fatal(err)
	}
	linkedRoot := filepath.Join(base, "linked")
	if err := os.Symlink(realRoot, linkedRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareMountpoint(linkedRoot, "tenant", "", false); err == nil {
		t.Fatal("symlinked mount root was accepted")
	}
}

func TestDecodeMountInfoPath(t *testing.T) {
	value := decodeMountInfoPath(`/var/lib/dofs\040mounts/name\134value`)
	if value != `/var/lib/dofs mounts/name\value` {
		t.Fatalf("decoded path = %q", value)
	}
}

//go:build linux

package dofs_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/willvar/dofs"
)

func TestRealFUSEUnlinkDefersReclaimUntilLastHandleCloses(t *testing.T) {
	if os.Getenv("DOFS_FUSE_INTEGRATION") != "1" {
		t.Skip("set DOFS_FUSE_INTEGRATION=1 to exercise the kernel FUSE contract")
	}
	backend, metadata, objects := newBackend(t, true)
	if err := backend.EnableWriteback(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := dofsMountForTest(mountpoint, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = mounted.Unmount()
		mounted.Wait()
	}()

	content := []byte("an unlinked open file must remain readable")
	path := filepath.Join(mountpoint, "open-then-delete.txt")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	node, err := backend.Lookup(t.Context(), dofs.RootInode, filepath.Base(path))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if _, exists := objects.Bytes(node.ObjectKey); !exists {
		_ = reader.Close()
		t.Fatal("ciphertext was reclaimed while an open handle still referenced it")
	}
	_, err = os.ReadFile(reader.Name())
	if !errors.Is(err, os.ErrNotExist) {
		_ = reader.Close()
		t.Fatalf("unlinked path remains visible: %v", err)
	}
	buffer := make([]byte, len(content))
	if _, err := reader.ReadAt(buffer, 0); err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if string(buffer) != string(content) {
		_ = reader.Close()
		t.Fatalf("open-handle content = %q", buffer)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, exists := objects.Bytes(node.ObjectKey); !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ciphertext remains after the last open handle closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := metadata.GetNode(t.Context(), "tenant-a", node.Inode); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("tombstone after last close = %v", err)
	}
	namespace, err := metadata.GetNamespace(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if namespace.UsedBytes != 0 || namespace.PendingReclaimBytes != 0 {
		t.Fatalf("usage after last close = used %d, pending %d", namespace.UsedBytes, namespace.PendingReclaimBytes)
	}
}

func TestRealFUSECrossHandleWriteVisibility(t *testing.T) {
	if os.Getenv("DOFS_FUSE_INTEGRATION") != "1" {
		t.Skip("set DOFS_FUSE_INTEGRATION=1 to exercise the kernel FUSE contract")
	}
	backend, _, _ := newBackend(t, true)
	if err := backend.EnableWriteback(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := dofsMountForTest(mountpoint, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = mounted.Unmount()
		mounted.Wait()
	}()

	filePath := filepath.Join(mountpoint, "visible.txt")
	writer, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("visible before fsync or close")
	if _, err := writer.Write(content); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	got, err := os.ReadFile(filePath)
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if string(got) != string(content) {
		_ = writer.Close()
		t.Fatalf("cross-handle read = %q, want %q", got, content)
	}
	if err := os.Truncate(filePath, int64(len("visible"))); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if _, err := writer.WriteAt([]byte("-again"), int64(len("visible"))); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	got, err = os.ReadFile(filePath)
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if want := "visible-again"; string(got) != want {
		_ = writer.Close()
		t.Fatalf("cross-handle truncate/write = %q, want %q", got, want)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

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

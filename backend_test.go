package dofs_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/willvar/dofs"
	metadatamemory "github.com/willvar/dofs/metadata/memory"
	objectmemory "github.com/willvar/dofs/object/memory"
)

func newBackend(t *testing.T, writable bool) (*dofs.Backend, *metadatamemory.Store, *objectmemory.Store) {
	return newBackendWithOptions(t, dofs.BackendOptions{Writable: writable})
}

func newBackendWithOptions(t *testing.T, options dofs.BackendOptions) (*dofs.Backend, *metadatamemory.Store, *objectmemory.Store) {
	t.Helper()
	master := bytes.Repeat([]byte{0x42}, dofs.KeySize)
	provider, err := dofs.NewEnvelopeKeyProvider(master)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(provider.Close)
	wrapped, err := provider.WrapNewNamespaceKey()
	if err != nil {
		t.Fatal(err)
	}
	metadata := metadatamemory.New()
	if _, err := metadata.CreateNamespace(t.Context(), dofs.Namespace{ID: "tenant-a", WrappedKEK: wrapped}); err != nil {
		t.Fatal(err)
	}
	objects := objectmemory.New()
	backend, err := dofs.NewBackend(t.Context(), "tenant-a", metadata, objects, provider, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return backend, metadata, objects
}

func putDirectUploadFixture(t *testing.T, objects *objectmemory.Store, upload dofs.ExternalUpload, key, plaintext []byte) {
	t.Helper()
	var encrypted bytes.Buffer
	if err := dofs.EncryptStream(key, bytes.NewReader(plaintext), &encrypted); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(t.Context(), upload.ObjectKey, bytes.NewReader(encrypted.Bytes()), int64(encrypted.Len())); err != nil {
		t.Fatal(err)
	}
}

func TestDirectUploadPublishesWithoutWriterLease(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	if backend.Writable() || !backend.CanMutate() {
		t.Fatalf("writer = %v, mutations = %v", backend.Writable(), backend.CanMutate())
	}
	lease, err := metadata.AcquireWriterLease(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()

	key := bytes.Repeat([]byte{0x33}, dofs.KeySize)
	plaintext := bytes.Repeat([]byte("direct-to-object-storage"), 4097)
	upload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "browser-upload-1", Parent: dofs.RootInode, Name: "video.bin",
		Mode: 0640, Size: int64(len(plaintext)), FileKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !upload.CreatesNode || upload.ExpectedGeneration != 0 || upload.TargetGeneration != 1 {
		t.Fatalf("upload = %#v", upload)
	}
	refreshed, err := backend.RefreshDirectUpload(t.Context(), upload.ID, 4*time.Hour)
	if err != nil || !refreshed.ExpiresAt.After(upload.ExpiresAt) {
		t.Fatalf("refreshed upload = %#v, %v", refreshed, err)
	}
	if _, err := backend.Lookup(t.Context(), dofs.RootInode, "video.bin"); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("creating node is visible: %v", err)
	}
	putDirectUploadFixture(t, objects, upload, key, plaintext)
	node, err := backend.CommitDirectUpload(t.Context(), upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if node.Inode != upload.Inode || node.Generation != 1 || node.Size != int64(len(plaintext)) {
		t.Fatalf("node = %#v", node)
	}
	read, err := backend.ReadAt(t.Context(), node, 0, len(plaintext))
	if err != nil || !bytes.Equal(read, plaintext) {
		t.Fatalf("read differs: %v", err)
	}
	// Commit remains idempotent while the application persists its own
	// projection/task state.
	if retry, err := backend.CommitDirectUpload(t.Context(), upload.ID); err != nil || retry.Inode != node.Inode {
		t.Fatalf("retry commit = %#v, %v", retry, err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.GetDirectUpload(t.Context(), upload.ID); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("acknowledged upload = %v", err)
	}
}

func TestDirectUploadReplacementUsesGenerationCAS(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	firstKey := bytes.Repeat([]byte{0x11}, dofs.KeySize)
	first, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "first", Parent: dofs.RootInode, Name: "document.txt", Size: 5, FileKey: firstKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, first, firstKey, []byte("first"))
	node, err := backend.CommitDirectUpload(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}

	replacementKey := bytes.Repeat([]byte{0x22}, dofs.KeySize)
	replacement, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "replacement", Parent: dofs.RootInode, Name: "document.txt", Size: 11,
		FileKey: replacementKey, Replace: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, replacement, replacementKey, []byte("replacement"))

	// Simulate a FUSE writer winning generation 2 after the browser reserved
	// it. The browser commit must not overwrite that generation.
	concurrentKey, _ := dofs.GenerationObjectKey("tenant-a", node.Inode, 2, "fuse-writer")
	var encrypted bytes.Buffer
	if err := dofs.EncryptStream(firstKey, bytes.NewReader([]byte("fuse")), &encrypted); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(t.Context(), concurrentKey, bytes.NewReader(encrypted.Bytes()), int64(encrypted.Len())); err != nil {
		t.Fatal(err)
	}
	concurrent, switched, err := metadata.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: "tenant-a", Inode: node.Inode, ExpectedGeneration: 1,
		ObjectKey: concurrentKey, Size: 4,
	})
	if err != nil || !switched {
		t.Fatalf("concurrent commit = %#v, %v, %v", concurrent, switched, err)
	}
	if _, err := backend.CommitDirectUpload(t.Context(), replacement.ID); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("replacement commit = %v", err)
	}
	if err := backend.AbortDirectUpload(t.Context(), replacement.ID); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.Bytes(replacement.ObjectKey); exists {
		t.Fatal("conflicting direct upload object was not removed")
	}
	read, err := backend.ReadAt(t.Context(), concurrent, 0, 4)
	if err != nil || string(read) != "fuse" {
		t.Fatalf("winning generation = %q, %v", read, err)
	}
}

func TestDirectUploadRejectsWrongCiphertextGeometry(t *testing.T) {
	backend, _, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	key := bytes.Repeat([]byte{0x44}, dofs.KeySize)
	upload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "short-object", Parent: dofs.RootInode, Name: "short.bin", Size: 10, FileKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	header := []byte{dofs.CryptoVersion, 0, 1, 0, 0}
	if err := objects.Put(t.Context(), upload.ObjectKey, bytes.NewReader(header), int64(len(header))); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.CommitDirectUpload(t.Context(), upload.ID); err == nil {
		t.Fatal("short ciphertext was published")
	}
	if err := backend.AbortDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}
}

func TestBackendEncryptedCreateLookupAndRead(t *testing.T) {
	backend, _, objects := newBackend(t, true)
	directory, err := backend.CreateDirectory(t.Context(), dofs.RootInode, "docs", 0750)
	if err != nil {
		t.Fatal(err)
	}
	file, err := backend.CreateFile(t.Context(), directory.Inode, "empty.txt", 0640)
	if err != nil {
		t.Fatal(err)
	}
	if file.Generation != 1 || file.Size != 0 || file.Mode != 0640 {
		t.Fatalf("file = %#v", file)
	}
	if encrypted, exists := objects.Bytes(file.ObjectKey); !exists || len(encrypted) != dofs.EncryptedHeaderSize {
		t.Fatalf("encrypted empty object = %d, %v", len(encrypted), exists)
	}
	children, err := backend.List(t.Context(), directory.Inode)
	if err != nil || len(children) != 1 || children[0].Inode != file.Inode {
		t.Fatalf("children = %#v, %v", children, err)
	}
	plaintext, err := backend.ReadAt(t.Context(), file, 0, 32)
	if err != nil || len(plaintext) != 0 {
		t.Fatalf("read = %q, %v", plaintext, err)
	}
	if _, err := backend.Chmod(t.Context(), file.Inode, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestBackendRangeReadExistingGeneration(t *testing.T) {
	backend, metadata, objects := newBackend(t, true)
	file, err := backend.CreateFile(t.Context(), dofs.RootInode, "data.bin", 0600)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), dofs.DefaultChunkSize/8+29)
	// Reuse the file DEK through the public envelope primitives to publish a
	// fixture generation without relying on a write handle.
	master := bytes.Repeat([]byte{0x42}, dofs.KeySize)
	provider, _ := dofs.NewEnvelopeKeyProvider(master)
	defer provider.Close()
	namespace, _ := metadata.GetNamespace(t.Context(), "tenant-a")
	kek, err := provider.UnwrapNamespaceKey(t.Context(), namespace)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := dofs.UnwrapKey(kek, file.WrappedDEK)
	dofs.Clear(kek)
	if err != nil {
		t.Fatal(err)
	}
	var encrypted bytes.Buffer
	if err := dofs.EncryptStream(dek, bytes.NewReader(plaintext), &encrypted); err != nil {
		t.Fatal(err)
	}
	dofs.Clear(dek)
	key, _ := dofs.GenerationObjectKey("tenant-a", file.Inode, 2, "fixture")
	if err := objects.Put(context.Background(), key, bytes.NewReader(encrypted.Bytes()), int64(encrypted.Len())); err != nil {
		t.Fatal(err)
	}
	file, ok, err := metadata.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: "tenant-a", Inode: file.Inode, ExpectedGeneration: 1, ObjectKey: key, Size: int64(len(plaintext)),
	})
	if err != nil || !ok {
		t.Fatalf("commit = %v, %v", ok, err)
	}
	offset := int64(dofs.DefaultChunkSize - 17)
	read, err := backend.ReadAt(t.Context(), file, offset, 73)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read, plaintext[offset:offset+73]) {
		t.Fatal("range differs")
	}
	stream, err := backend.OpenPlaintext(t.Context(), file)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var all bytes.Buffer
	if _, err := all.ReadFrom(stream); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(all.Bytes(), plaintext) {
		t.Fatal("stream differs")
	}
}

func TestBackendRenameReplacementAndDeletionCollectObjects(t *testing.T) {
	backend, metadata, objects := newBackend(t, true)
	first, _ := backend.CreateFile(t.Context(), dofs.RootInode, "first", 0600)
	second, _ := backend.CreateFile(t.Context(), dofs.RootInode, "second", 0600)
	if _, err := backend.Rename(t.Context(), first.Inode, dofs.RootInode, "second", false); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.Bytes(second.ObjectKey); exists {
		t.Fatal("replaced inode object was not collected")
	}
	if _, err := metadata.GetNode(t.Context(), "tenant-a", second.Inode); err == nil {
		t.Fatal("replaced inode metadata was not purged")
	}
	if err := backend.Remove(t.Context(), dofs.RootInode, "second", false); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.Bytes(first.ObjectKey); exists {
		t.Fatal("removed inode object was not collected")
	}
}

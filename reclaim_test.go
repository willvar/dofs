package dofs_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/willvar/dofs"
)

type transientDeleteStore struct {
	dofs.ObjectStore
	failures int
}

func (s *transientDeleteStore) Delete(ctx context.Context, key string) error {
	if s.failures > 0 {
		s.failures--
		return errors.New("temporary object-store failure")
	}
	return s.ObjectStore.Delete(ctx, key)
}

func TestControlPlaneDeletionReclaimsDurablyOutsideFUSE(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	fileKey := bytes.Repeat([]byte{0x65}, dofs.KeySize)
	plaintext := []byte("permanent deletion must release quota and object storage")
	upload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "reclaim-source", Parent: dofs.RootInode, Name: "source.txt",
		Size: int64(len(plaintext)), FileKey: fileKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, upload, fileKey, plaintext)
	node, err := backend.CommitDirectUpload(t.Context(), upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}

	mount, err := metadata.AcquireMountLease(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(t.Context(), dofs.RootInode, node.Name, false); err != nil {
		t.Fatal(err)
	}
	namespace, err := metadata.GetNamespace(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if namespace.UsedBytes != 0 || namespace.PendingReclaimBytes != int64(len(plaintext)) {
		t.Fatalf("usage after permanent delete = used %d, pending %d", namespace.UsedBytes, namespace.PendingReclaimBytes)
	}
	if _, exists := objects.Bytes(node.ObjectKey); !exists {
		t.Fatal("object was reclaimed while a mount lease was active")
	}

	busy, err := dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a")
	if err != nil || !busy.Busy || busy.Purged != 0 {
		t.Fatalf("busy reclaim = %#v, %v", busy, err)
	}
	if err := mount.Close(); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a")
	if err != nil || reclaimed.Scanned != 1 || reclaimed.Purged != 1 || reclaimed.Busy {
		t.Fatalf("reclaim = %#v, %v", reclaimed, err)
	}
	if _, exists := objects.Bytes(node.ObjectKey); exists {
		t.Fatal("permanently deleted object remains")
	}
	if _, err := metadata.GetNode(t.Context(), "tenant-a", node.Inode); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("purged inode = %v", err)
	}
	namespace, err = metadata.GetNamespace(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if namespace.UsedBytes != 0 || namespace.PendingReclaimBytes != 0 {
		t.Fatalf("usage after reclaim = used %d, pending %d", namespace.UsedBytes, namespace.PendingReclaimBytes)
	}
	retry, err := dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a")
	if err != nil || retry.Scanned != 0 || retry.Purged != 0 {
		t.Fatalf("idempotent reclaim = %#v, %v", retry, err)
	}
}

func TestReclaimerDefersLiveDirectUploadURL(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	fileKey := bytes.Repeat([]byte{0x44}, dofs.KeySize)
	first, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "initial", Parent: dofs.RootInode, Name: "replace.bin", Size: 5, FileKey: fileKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, first, fileKey, []byte("first"))
	if _, err := backend.CommitDirectUpload(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	replacement, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "late-replacement", Parent: dofs.RootInode, Name: "replace.bin", Size: 6,
		FileKey: bytes.Repeat([]byte{0x45}, dofs.KeySize), Replace: true, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(t.Context(), dofs.RootInode, "replace.bin", false); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.RefreshDirectUpload(t.Context(), replacement.ID, 2*time.Hour); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("refresh deleted inode upload = %v", err)
	}
	if _, err := metadata.Purge(t.Context(), "tenant-a", first.Inode, 0); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("purge with stale generation = %v", err)
	}
	if _, err := metadata.Purge(t.Context(), "tenant-a", first.Inode, 1); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("purge with live upload URL = %v", err)
	}
	result, err := dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a")
	if err != nil || result.Deferred != 1 || result.Purged != 0 {
		t.Fatalf("deferred reclaim = %#v, %v", result, err)
	}
	if err := backend.AbortDirectUpload(t.Context(), replacement.ID); err != nil {
		t.Fatal(err)
	}
	result, err = dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a")
	if err != nil || result.Purged != 1 {
		t.Fatalf("reclaim after abort = %#v, %v", result, err)
	}
}

func TestMountLeaseIsExclusiveAndExcludesReclamation(t *testing.T) {
	_, metadata, _ := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	first, err := metadata.AcquireMountLease(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.AcquireMountLease(t.Context(), "tenant-a"); !errors.Is(err, dofs.ErrMountBusy) {
		t.Fatalf("second mount = %v", err)
	}
	if _, err := metadata.AcquireReclaimLease(t.Context(), "tenant-a"); !errors.Is(err, dofs.ErrReclaimBusy) {
		t.Fatalf("reclamation with mount = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reclaimer, err := metadata.AcquireReclaimLease(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.AcquireMountLease(t.Context(), "tenant-a"); !errors.Is(err, dofs.ErrMountBusy) {
		t.Fatalf("mount during reclamation = %v", err)
	}
	if err := reclaimer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMountedBackendDrainsControlPlaneTombstonesLocally(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{
		Writable: true, MountedReclaimInterval: 5 * time.Millisecond,
	})
	fileKey := bytes.Repeat([]byte{0x5a}, dofs.KeySize)
	plaintext := []byte("mounted collector")
	upload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "mounted-collector", Parent: dofs.RootInode, Name: "mounted.txt",
		Size: int64(len(plaintext)), FileKey: fileKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, upload, fileKey, plaintext)
	node, err := backend.CommitDirectUpload(t.Context(), upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}
	// Mutate metadata directly to model a separate browser/API control-plane
	// process, which cannot signal this mount's in-memory handle registry.
	if _, err := metadata.Remove(t.Context(), dofs.RemoveRequest{
		NamespaceID: "tenant-a", Parent: dofs.RootInode, Name: node.Name,
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a"); err != nil || !result.Busy {
		t.Fatalf("external reclaim while mounted = %#v, %v", result, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, exists := objects.Bytes(node.ObjectKey); !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mounted collector did not reclaim the tombstone")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := metadata.GetNode(t.Context(), "tenant-a", node.Inode); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("mounted collector inode = %v", err)
	}
	namespace, err := metadata.GetNamespace(t.Context(), "tenant-a")
	if err != nil || namespace.UsedBytes != 0 || namespace.PendingReclaimBytes != 0 {
		t.Fatalf("mounted collector usage = %#v, %v", namespace, err)
	}
}

func TestReclaimerPrunesSupersededGenerations(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	fileKey := bytes.Repeat([]byte{0x29}, dofs.KeySize)
	first, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "generation-one", Parent: dofs.RootInode, Name: "mutable.bin", Size: 3, FileKey: fileKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, first, fileKey, []byte("one"))
	node, err := backend.CommitDirectUpload(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	secondKey, err := dofs.GenerationObjectKey("tenant-a", node.Inode, 2, "generation-two")
	if err != nil {
		t.Fatal(err)
	}
	var secondCiphertext bytes.Buffer
	if err := dofs.EncryptStream(fileKey, bytes.NewReader([]byte("two-two")), &secondCiphertext); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(t.Context(), secondKey, bytes.NewReader(secondCiphertext.Bytes()), int64(secondCiphertext.Len())); err != nil {
		t.Fatal(err)
	}
	node, committed, err := metadata.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: "tenant-a", Inode: node.Inode, ExpectedGeneration: 1,
		ObjectKey: secondKey, Size: 7,
	})
	if err != nil || !committed {
		t.Fatalf("second generation = %#v, %v, %v", node, committed, err)
	}
	if _, err := metadata.DeleteGeneration(t.Context(), "tenant-a", node.Inode, node.Generation); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("delete current generation = %v", err)
	}
	deferred, err := dofs.ReclaimDeleted(t.Context(), metadata, objects, "tenant-a")
	if err != nil || deferred.PrunedGenerations != 0 || deferred.DeferredGenerations != 1 {
		t.Fatalf("presigned-read grace = %#v, %v", deferred, err)
	}
	if _, exists := objects.Bytes(first.ObjectKey); !exists {
		t.Fatal("superseded generation was removed before the read grace elapsed")
	}
	result, err := dofs.Reclaim(t.Context(), metadata, objects, "tenant-a", dofs.ReclaimOptions{})
	if err != nil || result.PrunedGenerations != 1 || result.Purged != 0 {
		t.Fatalf("generation reclaim = %#v, %v", result, err)
	}
	if _, exists := objects.Bytes(first.ObjectKey); exists {
		t.Fatal("superseded generation remained")
	}
	if _, exists := objects.Bytes(secondKey); !exists {
		t.Fatal("current generation was removed")
	}
	generations, err := metadata.ListGenerations(t.Context(), "tenant-a", node.Inode)
	if err != nil || len(generations) != 1 || generations[0].Number != node.Generation {
		t.Fatalf("remaining generations = %#v, %v", generations, err)
	}
}

func TestReclaimerRetriesObjectStoreFailureFromTombstone(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	fileKey := bytes.Repeat([]byte{0x71}, dofs.KeySize)
	upload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "retry-delete", Parent: dofs.RootInode, Name: "retry.bin", Size: 5, FileKey: fileKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, upload, fileKey, []byte("retry"))
	node, err := backend.CommitDirectUpload(t.Context(), upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(t.Context(), dofs.RootInode, node.Name, false); err != nil {
		t.Fatal(err)
	}
	flaky := &transientDeleteStore{ObjectStore: objects, failures: 1}
	if result, err := dofs.ReclaimDeleted(t.Context(), metadata, flaky, "tenant-a"); err == nil || result.Purged != 0 {
		t.Fatalf("failed reclaim = %#v, %v", result, err)
	}
	if _, err := metadata.GetNode(t.Context(), "tenant-a", node.Inode); err != nil {
		t.Fatalf("tombstone was lost after object failure: %v", err)
	}
	result, err := dofs.ReclaimDeleted(t.Context(), metadata, flaky, "tenant-a")
	if err != nil || result.Purged != 1 {
		t.Fatalf("retried reclaim = %#v, %v", result, err)
	}
}

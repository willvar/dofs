package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/willvar/dofs"
	"github.com/willvar/dofs/metadata/memory"
)

func newNamespace(t *testing.T, quota int64) (*memory.Store, dofs.Namespace) {
	t.Helper()
	store := memory.New()
	namespace, err := store.CreateNamespace(t.Context(), dofs.Namespace{
		ID: "tenant-a", Label: "Tenant A", WrappedKEK: []byte("wrapped"), QuotaBytes: quota,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, namespace
}

func TestNamespaceLifecycleAndGenerationCAS(t *testing.T) {
	store, namespace := newNamespace(t, 10)
	directory, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespace.ID, Parent: dofs.RootInode, Name: "docs", Kind: dofs.NodeKindDirectory,
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespace.ID, Parent: directory.Inode, Name: "note.md", Kind: dofs.NodeKindFile,
		WrappedDEK: []byte("dek"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Lookup(t.Context(), namespace.ID, directory.Inode, file.Name); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("creating file lookup error = %v", err)
	}
	committed, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespace.ID, Inode: file.Inode, ExpectedGeneration: 0,
		ObjectKey: generationKey(t, namespace.ID, file.Inode, 1, "object-1"), Size: 7,
	})
	if err != nil || !ok || committed.Generation != 1 || committed.State != dofs.NodeStateReady {
		t.Fatalf("commit = %#v, %v, %v", committed, ok, err)
	}
	if current, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespace.ID, Inode: file.Inode, ExpectedGeneration: 0,
		ObjectKey: generationKey(t, namespace.ID, file.Inode, 1, "stale"), Size: 1,
	}); err != nil || ok || current.ObjectKey != committed.ObjectKey {
		t.Fatalf("stale commit = %#v, %v, %v", current, ok, err)
	}
	if _, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespace.ID, Inode: file.Inode, ExpectedGeneration: 1,
		ObjectKey: generationKey(t, namespace.ID, file.Inode, 2, "too-large"), Size: 11,
	}); !errors.Is(err, dofs.ErrQuotaExceeded) || ok {
		t.Fatalf("quota commit = %v, %v", ok, err)
	}
	currentNamespace, err := store.GetNamespace(t.Context(), namespace.ID)
	if err != nil || currentNamespace.UsedBytes != 7 {
		t.Fatalf("namespace = %#v, %v", currentNamespace, err)
	}
	events, err := store.ListEvents(t.Context(), namespace.ID, 0, 100)
	if err != nil || len(events) != 2 || events[0].Kind != dofs.EventCreate || events[1].Kind != dofs.EventCreate {
		t.Fatalf("events = %#v, %v", events, err)
	}
}

func TestDeleteNamespaceRequiresReleasedWriterLease(t *testing.T) {
	store, namespace := newNamespace(t, 0)
	lease, err := store.AcquireWriterLease(t.Context(), namespace.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNamespace(t.Context(), namespace.ID); !errors.Is(err, dofs.ErrWriterBusy) {
		t.Fatalf("delete mounted namespace error = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNamespace(t.Context(), namespace.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetNamespace(t.Context(), namespace.ID); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("deleted namespace = %v", err)
	}
}

func TestRenameReplaceRemoveAndPurge(t *testing.T) {
	store, namespace := newNamespace(t, 0)
	first := createReadyFile(t, store, namespace.ID, "first", 4)
	second := createReadyFile(t, store, namespace.ID, "second", 5)
	renamed, replaced, err := store.Rename(t.Context(), dofs.RenameRequest{
		NamespaceID: namespace.ID, Inode: first.Inode, NewParent: dofs.RootInode, NewName: second.Name,
	})
	if err != nil || renamed.Name != "second" || replaced == nil || replaced.Inode != second.Inode || replaced.State != dofs.NodeStateDeleted {
		t.Fatalf("rename = %#v, %#v, %v", renamed, replaced, err)
	}
	events, err := store.ListEvents(t.Context(), namespace.ID, 0, 100)
	if err != nil || len(events) != 4 || events[2].Kind != dofs.EventRemove || events[2].Inode != second.Inode || events[3].Kind != dofs.EventRename {
		t.Fatalf("rename events = %#v, %v", events, err)
	}
	removed, err := store.Remove(t.Context(), dofs.RemoveRequest{
		NamespaceID: namespace.ID, Parent: dofs.RootInode, Name: renamed.Name,
	})
	if err != nil || removed.State != dofs.NodeStateDeleted {
		t.Fatalf("remove = %#v, %v", removed, err)
	}
	for _, inode := range []uint64{second.Inode, first.Inode} {
		purged, err := store.Purge(t.Context(), namespace.ID, inode)
		if err != nil || !purged {
			t.Fatalf("purge %d = %v, %v", inode, purged, err)
		}
	}
	currentNamespace, _ := store.GetNamespace(t.Context(), namespace.ID)
	if currentNamespace.UsedBytes != 0 {
		t.Fatalf("used bytes = %d", currentNamespace.UsedBytes)
	}
}

func TestDirectoryCycleAndWriterLease(t *testing.T) {
	store, namespace := newNamespace(t, 0)
	parent, _ := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespace.ID, Parent: dofs.RootInode, Name: "parent", Kind: dofs.NodeKindDirectory,
	})
	child, _ := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespace.ID, Parent: parent.Inode, Name: "child", Kind: dofs.NodeKindDirectory,
	})
	if _, _, err := store.Rename(t.Context(), dofs.RenameRequest{
		NamespaceID: namespace.ID, Inode: parent.Inode, NewParent: child.Inode, NewName: "cycle",
	}); !errors.Is(err, dofs.ErrInvalidName) {
		t.Fatalf("cycle error = %v", err)
	}
	lease, err := store.AcquireWriterLease(t.Context(), namespace.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireWriterLease(t.Context(), namespace.ID); !errors.Is(err, dofs.ErrWriterBusy) {
		t.Fatalf("second lease error = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireWriterLease(context.Background(), namespace.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Close()
}

func createReadyFile(t *testing.T, store *memory.Store, namespaceID, name string, size int64) dofs.Node {
	t.Helper()
	file, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespaceID, Parent: dofs.RootInode, Name: name, Kind: dofs.NodeKindFile, WrappedDEK: []byte("dek"),
	})
	if err != nil {
		t.Fatal(err)
	}
	file, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespaceID, Inode: file.Inode, ExpectedGeneration: 0,
		ObjectKey: generationKey(t, namespaceID, file.Inode, 1, "object-"+name), Size: size,
	})
	if err != nil || !ok {
		t.Fatalf("commit %s = %v, %v", name, ok, err)
	}
	return file
}

func generationKey(t *testing.T, namespaceID string, inode uint64, generation int64, transaction string) string {
	t.Helper()
	key, err := dofs.GenerationObjectKey(namespaceID, inode, generation, transaction)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

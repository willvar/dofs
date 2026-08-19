package dofs_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/willvar/dofs"
	metadatamemory "github.com/willvar/dofs/metadata/memory"
	objectmemory "github.com/willvar/dofs/object/memory"
)

func TestDeleteNamespaceIsScopedAndRetryable(t *testing.T) {
	metadata := metadatamemory.New()
	objects := objectmemory.New()
	for _, id := range []string{"delete-me", "keep-me"} {
		if _, err := metadata.CreateNamespace(t.Context(), dofs.Namespace{ID: id, WrappedKEK: bytes.Repeat([]byte{1}, 32)}); err != nil {
			t.Fatal(err)
		}
		prefix, err := dofs.ObjectRoot(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := objects.Put(t.Context(), prefix+"fixture", bytes.NewReader([]byte("ciphertext")), 10); err != nil {
			t.Fatal(err)
		}
	}
	mount, err := metadata.AcquireMountLease(t.Context(), "delete-me")
	if err != nil {
		t.Fatal(err)
	}
	if err := dofs.DeleteNamespace(t.Context(), metadata, objects, "delete-me"); !errors.Is(err, dofs.ErrReclaimBusy) {
		t.Fatalf("delete mounted namespace = %v", err)
	}
	if err := mount.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dofs.DeleteNamespace(t.Context(), metadata, objects, "delete-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.GetNamespace(t.Context(), "delete-me"); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("deleted namespace = %v", err)
	}
	deletedRoot, _ := dofs.ObjectRoot("delete-me")
	if _, exists := objects.Bytes(deletedRoot + "fixture"); exists {
		t.Fatal("deleted namespace object remained")
	}
	keptRoot, _ := dofs.ObjectRoot("keep-me")
	if _, exists := objects.Bytes(keptRoot + "fixture"); !exists {
		t.Fatal("neighbor namespace object was deleted")
	}
	if err := dofs.DeleteNamespace(t.Context(), metadata, objects, "delete-me"); err != nil {
		t.Fatalf("retry delete = %v", err)
	}
}

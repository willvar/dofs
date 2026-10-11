package dofs_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/willvar/dofs"
)

func TestGenerationHistoryUsesImmutableKeysAndRestoresByCopy(t *testing.T) {
	backend, metadata, objects := newBackendWithOptions(t, dofs.BackendOptions{ControlPlane: true})
	if _, err := metadata.SetHistoryLimit(t.Context(), "tenant-a", 2); err != nil {
		t.Fatal(err)
	}
	keyOne := bytes.Repeat([]byte{0x11}, dofs.KeySize)
	keyTwo := bytes.Repeat([]byte{0x22}, dofs.KeySize)
	firstPlaintext := []byte("first immutable version")
	secondPlaintext := []byte("second immutable version is longer")

	firstUpload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "history-first", Parent: dofs.RootInode, Name: "history.txt",
		Size: int64(len(firstPlaintext)), FileKey: keyOne,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, firstUpload, keyOne, firstPlaintext)
	firstNode, err := backend.CommitDirectUpload(t.Context(), firstUpload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), firstUpload.ID); err != nil {
		t.Fatal(err)
	}

	secondUpload, err := backend.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "history-second", Parent: dofs.RootInode, Name: "history.txt", Replace: true,
		Size: int64(len(secondPlaintext)), FileKey: keyTwo,
	})
	if err != nil {
		t.Fatal(err)
	}
	putDirectUploadFixture(t, objects, secondUpload, keyTwo, secondPlaintext)
	secondNode, err := backend.CommitDirectUpload(t.Context(), secondUpload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), secondUpload.ID); err != nil {
		t.Fatal(err)
	}

	generations, err := backend.ListGenerations(t.Context(), firstNode.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if len(generations) != 2 || generations[0].Number != 1 || generations[1].Number != 2 {
		t.Fatalf("generations = %#v", generations)
	}
	if !bytes.Equal(generations[0].WrappedDEK, firstUpload.WrappedDEK) ||
		!bytes.Equal(generations[1].WrappedDEK, secondUpload.WrappedDEK) {
		t.Fatalf("generation keys were not retained per version")
	}

	reader, err := backend.OpenGenerationPlaintext(t.Context(), firstNode.Inode, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstRead, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read generation one = %q, %v, %v", firstRead, err, closeErr)
	}
	if !bytes.Equal(firstRead, firstPlaintext) {
		t.Fatalf("generation one plaintext = %q, want %q", firstRead, firstPlaintext)
	}

	cloneUpload, err := backend.PrepareCiphertextClone(
		t.Context(), "history-copy", dofs.RootInode, "history-copy.txt", secondNode.Mode,
		secondNode, false, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Copy(t.Context(), secondNode.ObjectKey, cloneUpload.ObjectKey); err != nil {
		t.Fatal(err)
	}
	copyNode, err := backend.CommitDirectUpload(t.Context(), cloneUpload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.AcknowledgeDirectUpload(t.Context(), cloneUpload.ID); err != nil {
		t.Fatal(err)
	}
	copyGenerations, err := backend.ListGenerations(t.Context(), copyNode.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if len(copyGenerations) != 1 || copyGenerations[0].Number != 1 {
		t.Fatalf("copied generations = %#v", copyGenerations)
	}
	reader, err = backend.OpenPlaintext(t.Context(), copyNode)
	if err != nil {
		t.Fatal(err)
	}
	copyRead, err := io.ReadAll(reader)
	closeErr = reader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read copied current generation = %q, %v, %v", copyRead, err, closeErr)
	}
	if !bytes.Equal(copyRead, secondPlaintext) {
		t.Fatalf("copied plaintext = %q, want %q", copyRead, secondPlaintext)
	}

	restored, err := backend.RestoreGeneration(t.Context(), firstNode.Inode, 1, secondNode.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.WrappedDEK, generations[0].WrappedDEK) {
		t.Fatalf("restored wrapped key = %x, want %x", restored.WrappedDEK, generations[0].WrappedDEK)
	}
	if restored.Generation != 3 || restored.Size != int64(len(firstPlaintext)) {
		t.Fatalf("restored node = %#v", restored)
	}
	reader, err = backend.OpenPlaintext(t.Context(), restored)
	if err != nil {
		t.Fatal(err)
	}
	restoredRead, err := io.ReadAll(reader)
	closeErr = reader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read restored generation = %q, %v, %v", restoredRead, err, closeErr)
	}
	if !bytes.Equal(restoredRead, firstPlaintext) {
		t.Fatalf("restored plaintext = %q, want %q", restoredRead, firstPlaintext)
	}
	if _, err := backend.RestoreGeneration(t.Context(), firstNode.Inode, 1, secondNode.Generation); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("stale restore = %v, want generation conflict", err)
	}

	result, err := dofs.Reclaim(t.Context(), metadata, objects, "tenant-a", dofs.ReclaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.PrunedGenerations != 1 {
		t.Fatalf("reclaim result = %#v, want one pruned generation", result)
	}
	if _, err := metadata.GetNode(t.Context(), "tenant-a", firstNode.Inode); err != nil {
		t.Fatal(err)
	}
	remaining, err := metadata.ListGenerations(t.Context(), "tenant-a", firstNode.Inode)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 2 || remaining[0].Number != 2 || remaining[1].Number != 3 {
		t.Fatalf("remaining generations = %#v", remaining)
	}
	namespace, err := metadata.GetNamespace(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if namespace.HistoryBytes != int64(len(secondPlaintext)) {
		t.Fatalf("history bytes = %d, want %d", namespace.HistoryBytes, len(secondPlaintext))
	}
}

package dofs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/willvar/dofs"
	metadatamemory "github.com/willvar/dofs/metadata/memory"
	objectmemory "github.com/willvar/dofs/object/memory"
)

func TestWritebackPartialWriteTruncateAndGrow(t *testing.T) {
	backend, _, _ := newBackend(t, true)
	if err := backend.EnableWriteback(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	file, err := backend.CreateFile(t.Context(), dofs.RootInode, "data.bin", 0600)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Repeat([]byte("0123456789abcdef"), dofs.DefaultChunkSize/8+41)
	file = writeWholeFile(t, backend, file, original)

	var committed dofs.Node
	session, err := backend.OpenWrite(t.Context(), file, func(node dofs.Node) { committed = node })
	if err != nil {
		t.Fatal(err)
	}
	patchOffset := int64(dofs.DefaultChunkSize - 23)
	patch := bytes.Repeat([]byte{0x7f}, 91)
	if written, err := session.WriteAt(t.Context(), patch, patchOffset); err != nil || written != len(patch) {
		t.Fatalf("write = %d, %v", written, err)
	}
	probe := make([]byte, 121)
	if read, err := session.ReadAt(t.Context(), probe, patchOffset-15); err != nil || read != len(probe) {
		t.Fatalf("read = %d, %v", read, err)
	}
	wantProbe := append([]byte(nil), original[patchOffset-15:patchOffset-15+121]...)
	copy(wantProbe[15:], patch)
	if !bytes.Equal(probe, wantProbe) {
		t.Fatal("write session did not overlay dirty range")
	}
	if err := session.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if committed.Generation != file.Generation+1 {
		t.Fatalf("committed generation = %d", committed.Generation)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), original...)
	copy(want[patchOffset:], patch)
	assertPlaintext(t, backend, committed, want)

	file = committed
	session, err = backend.OpenWrite(t.Context(), file, func(node dofs.Node) { committed = node })
	if err != nil {
		t.Fatal(err)
	}
	shrink := int64(127)
	if err := session.Truncate(t.Context(), shrink); err != nil {
		t.Fatal(err)
	}
	grow := int64(len(want) + 53)
	if err := session.Truncate(t.Context(), grow); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	resized := make([]byte, grow)
	copy(resized, want[:shrink])
	assertPlaintext(t, backend, committed, resized)
}

func TestWritebackGenerationConflictDeletesLosingObject(t *testing.T) {
	backend, metadata, objects := newBackend(t, true)
	if err := backend.EnableWriteback(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	file, _ := backend.CreateFile(t.Context(), dofs.RootInode, "conflict", 0600)
	file = writeWholeFile(t, backend, file, []byte("base"))
	session, err := backend.OpenWrite(t.Context(), file, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.WriteAt(t.Context(), []byte("loser"), 0); err != nil {
		t.Fatal(err)
	}

	winner := publishFixtureGeneration(t, metadata, objects, file, []byte("winner"), "winner")
	if err := session.Commit(t.Context()); !errors.Is(err, dofs.ErrWriteConflict) {
		t.Fatalf("commit error = %v", err)
	}
	if err := session.Close(t.Context()); !errors.Is(err, dofs.ErrWriteConflict) {
		t.Fatalf("close error = %v", err)
	}
	assertPlaintext(t, backend, winner, []byte("winner"))
	generations, err := metadata.ListGenerations(t.Context(), "tenant-a", file.Inode)
	if err != nil || len(generations) != 3 {
		t.Fatalf("generations = %#v, %v", generations, err)
	}
}

func TestPreparedWALRecoversAfterUploadFailure(t *testing.T) {
	master := bytes.Repeat([]byte{0x31}, dofs.KeySize)
	provider, _ := dofs.NewEnvelopeKeyProvider(master)
	defer provider.Close()
	wrapped, _ := provider.WrapNewNamespaceKey()
	metadata := metadatamemory.New()
	_, _ = metadata.CreateNamespace(t.Context(), dofs.Namespace{ID: "recover", WrappedKEK: wrapped})
	baseObjects := objectmemory.New()
	objects := &failOnceObjectStore{Store: baseObjects}
	stateDirectory := t.TempDir()

	backend, err := dofs.NewBackend(t.Context(), "recover", metadata, objects, provider, dofs.BackendOptions{Writable: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.EnableWriteback(t.Context(), stateDirectory); err != nil {
		t.Fatal(err)
	}
	file, _ := backend.CreateFile(t.Context(), dofs.RootInode, "recover.txt", 0600)
	session, err := backend.OpenWrite(t.Context(), file, nil)
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("recoverable"), 1000)
	if _, err := session.WriteAt(t.Context(), content, 0); err != nil {
		t.Fatal(err)
	}
	objects.FailNextPut()
	if err := session.Commit(t.Context()); err == nil {
		t.Fatal("upload failure was not returned")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := dofs.NewBackend(t.Context(), "recover", metadata, objects, provider, dofs.BackendOptions{Writable: true})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.EnableWriteback(t.Context(), stateDirectory); err != nil {
		t.Fatal(err)
	}
	current, err := restarted.Lookup(t.Context(), dofs.RootInode, "recover.txt")
	if err != nil {
		t.Fatal(err)
	}
	assertPlaintext(t, restarted, current, content)
	session.Abort()
}

func writeWholeFile(t *testing.T, backend *dofs.Backend, file dofs.Node, content []byte) dofs.Node {
	t.Helper()
	committed := file
	session, err := backend.OpenWrite(t.Context(), file, func(node dofs.Node) { committed = node })
	if err != nil {
		t.Fatal(err)
	}
	if written, err := session.WriteAt(t.Context(), content, 0); err != nil || written != len(content) {
		t.Fatalf("write whole = %d, %v", written, err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	return committed
}

func assertPlaintext(t *testing.T, backend *dofs.Backend, node dofs.Node, want []byte) {
	t.Helper()
	reader, err := backend.OpenPlaintext(t.Context(), node)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read plaintext = %v, close = %v", err, closeErr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("plaintext length = %d, want %d", len(got), len(want))
	}
}

func publishFixtureGeneration(
	t *testing.T,
	metadata *metadatamemory.Store,
	objects *objectmemory.Store,
	file dofs.Node,
	content []byte,
	transaction string,
) dofs.Node {
	t.Helper()
	master := bytes.Repeat([]byte{0x42}, dofs.KeySize)
	provider, _ := dofs.NewEnvelopeKeyProvider(master)
	defer provider.Close()
	namespace, _ := metadata.GetNamespace(t.Context(), file.NamespaceID)
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
	if err := dofs.EncryptStream(dek, bytes.NewReader(content), &encrypted); err != nil {
		t.Fatal(err)
	}
	dofs.Clear(dek)
	key, _ := dofs.GenerationObjectKey(file.NamespaceID, file.Inode, file.Generation+1, transaction)
	if err := objects.Put(context.Background(), key, bytes.NewReader(encrypted.Bytes()), int64(encrypted.Len())); err != nil {
		t.Fatal(err)
	}
	current, ok, err := metadata.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: file.NamespaceID, Inode: file.Inode, ExpectedGeneration: file.Generation,
		ObjectKey: key, Size: int64(len(content)),
	})
	if err != nil || !ok {
		t.Fatalf("fixture commit = %v, %v", ok, err)
	}
	return current
}

type failOnceObjectStore struct {
	*dofsObjectStoreAdapter
	Store *objectmemory.Store
	mu    sync.Mutex
	fail  bool
}

// dofsObjectStoreAdapter exists only to prevent accidental method promotion
// from a future memory store API addition.
type dofsObjectStoreAdapter struct{}

func (s *failOnceObjectStore) FailNextPut() {
	s.mu.Lock()
	s.fail = true
	s.mu.Unlock()
}

func (s *failOnceObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	return s.Store.GetRange(ctx, key, start, end)
}

func (s *failOnceObjectStore) Stat(ctx context.Context, key string) (dofs.ObjectInfo, error) {
	return s.Store.Stat(ctx, key)
}

func (s *failOnceObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64) error {
	s.mu.Lock()
	fail := s.fail
	s.fail = false
	s.mu.Unlock()
	if fail {
		return errors.New("injected upload failure")
	}
	return s.Store.Put(ctx, key, reader, size)
}

func (s *failOnceObjectStore) Copy(ctx context.Context, sourceKey, destinationKey string) error {
	return s.Store.Copy(ctx, sourceKey, destinationKey)
}

func (s *failOnceObjectStore) Delete(ctx context.Context, key string) error {
	return s.Store.Delete(ctx, key)
}

func (s *failOnceObjectStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.Store.DeletePrefix(ctx, prefix)
}

var _ dofs.ObjectStore = (*failOnceObjectStore)(nil)

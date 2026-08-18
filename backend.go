package dofs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

type BackendOptions struct {
	// Writable acquires the namespace writer lease. It enables namespace
	// mutations; regular-file write handles additionally require a writeback
	// manager, configured by EnableWriteback.
	Writable bool
	// ControlPlane enables atomic namespace mutations without acquiring the
	// exclusive writeback lease. It is intended for API operations such as
	// direct-to-object-storage uploads that may safely race with one mounted
	// writer through metadata transactions and generation CAS.
	ControlPlane bool
}

type cachedFileKey struct {
	wrapper    []byte
	key        []byte
	generation int64
	chunkSize  int
}

// Backend exposes one namespace as plaintext inode operations. It is strictly
// namespace-scoped: callers cannot pass a second namespace to any method.
type Backend struct {
	namespace   Namespace
	metadata    MetadataStore
	objects     ObjectStore
	keys        KeyProvider
	kek         []byte
	lease       Lease
	writeback   *WritebackManager
	writebackMu sync.Mutex
	mutations   bool

	keyMu    sync.Mutex
	fileKeys map[uint64]*cachedFileKey

	handleMu sync.Mutex
	open     map[uint64]int
	closed   bool
}

func NewBackend(
	ctx context.Context,
	namespaceID string,
	metadata MetadataStore,
	objects ObjectStore,
	keys KeyProvider,
	options BackendOptions,
) (*Backend, error) {
	if err := ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if metadata == nil || objects == nil || keys == nil {
		return nil, errors.New("dofs: metadata, object store and key provider are required")
	}
	namespace, err := metadata.GetNamespace(ctx, namespaceID)
	if err != nil {
		return nil, fmt.Errorf("load DOFS namespace: %w", err)
	}
	root, err := metadata.GetNode(ctx, namespaceID, RootInode)
	if err != nil {
		return nil, fmt.Errorf("load DOFS root inode: %w", err)
	}
	if root.Inode != RootInode || !root.IsDir() || root.State != NodeStateReady || root.Parent != 0 {
		return nil, errors.New("dofs: invalid root inode")
	}
	kek, err := keys.UnwrapNamespaceKey(ctx, namespace)
	if err != nil {
		return nil, err
	}
	if len(kek) != KeySize {
		Clear(kek)
		return nil, errors.New("dofs: namespace key has invalid length")
	}
	backend := &Backend{
		namespace: namespace, metadata: metadata, objects: objects, keys: keys, kek: kek,
		fileKeys: make(map[uint64]*cachedFileKey), open: make(map[uint64]int),
		mutations: options.Writable || options.ControlPlane,
	}
	if options.Writable {
		lease, err := metadata.AcquireWriterLease(ctx, namespaceID)
		if err != nil {
			backend.Close()
			return nil, fmt.Errorf("acquire DOFS writer lease: %w", err)
		}
		backend.lease = lease
	}
	return backend, nil
}

func (b *Backend) Namespace() Namespace { return b.namespace }
func (b *Backend) Writable() bool       { return b != nil && b.lease != nil }
func (b *Backend) CanMutate() bool      { return b != nil && b.mutations }

func (b *Backend) LeaseLost() <-chan struct{} {
	if b == nil || b.lease == nil {
		return nil
	}
	return b.lease.Lost()
}

func (b *Backend) LeaseError() error {
	if b == nil || b.lease == nil {
		return nil
	}
	return b.lease.Err()
}

func (b *Backend) Close() error {
	if b == nil {
		return nil
	}
	b.handleMu.Lock()
	if b.closed {
		b.handleMu.Unlock()
		return nil
	}
	b.closed = true
	b.handleMu.Unlock()
	b.writebackMu.Lock()
	manager := b.writeback
	b.writeback = nil
	b.writebackMu.Unlock()
	if manager != nil {
		manager.Close()
	}
	b.keyMu.Lock()
	Clear(b.kek)
	b.kek = nil
	for inode, cached := range b.fileKeys {
		Clear(cached.key)
		Clear(cached.wrapper)
		delete(b.fileKeys, inode)
	}
	b.keyMu.Unlock()
	if b.lease != nil {
		return b.lease.Close()
	}
	return nil
}

// EnableWriteback attaches private local cache/WAL state and replays durable
// transactions before the backend is exposed to FUSE.
func (b *Backend) EnableWriteback(ctx context.Context, stateDirectory string) error {
	if err := b.requireWriter(); err != nil {
		return err
	}
	b.writebackMu.Lock()
	defer b.writebackMu.Unlock()
	if b.writeback != nil {
		return errors.New("dofs: writeback is already enabled")
	}
	manager, err := newWritebackManager(b, stateDirectory)
	if err != nil {
		return err
	}
	if err := manager.Recover(ctx); err != nil {
		manager.Close()
		return fmt.Errorf("recover DOFS writeback: %w", err)
	}
	if err := b.recoverNamespace(ctx); err != nil {
		manager.Close()
		return fmt.Errorf("recover DOFS namespace: %w", err)
	}
	b.writeback = manager
	return nil
}

func (b *Backend) OpenWrite(ctx context.Context, node Node, onCommit func(Node)) (*WriteSession, error) {
	b.writebackMu.Lock()
	manager := b.writeback
	b.writebackMu.Unlock()
	if manager == nil {
		return nil, ErrReadOnly
	}
	return manager.open(ctx, node, onCommit)
}

func (b *Backend) recoverNamespace(ctx context.Context) error {
	activeUploads, err := b.metadata.ListExternalUploads(ctx, b.namespace.ID, ExternalUploadActive)
	if err != nil {
		return err
	}
	protectedCreating := make(map[uint64]struct{}, len(activeUploads))
	now := time.Now().UTC()
	for _, upload := range activeUploads {
		if err := b.validateExternalUpload(upload); err != nil {
			return err
		}
		if now.Before(upload.ExpiresAt) {
			if upload.CreatesNode {
				protectedCreating[upload.Inode] = struct{}{}
			}
			continue
		}
		aborted, err := b.metadata.AbortExternalUpload(ctx, b.namespace.ID, upload.ID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("abort expired direct upload %s: %w", upload.ID, err)
		}
		if err == nil {
			if err := b.objects.Delete(ctx, aborted.ObjectKey); err != nil && !errors.Is(err, ErrObjectNotFound) {
				return fmt.Errorf("delete expired direct upload %s: %w", upload.ID, err)
			}
		}
	}
	creating, err := b.metadata.ListState(ctx, b.namespace.ID, NodeStateCreating)
	if err != nil {
		return err
	}
	for _, node := range creating {
		if _, protected := protectedCreating[node.Inode]; protected {
			continue
		}
		prefix, err := InodeObjectRoot(b.namespace.ID, node.Inode)
		if err != nil {
			return err
		}
		if err := b.objects.DeletePrefix(ctx, prefix); err != nil && !errors.Is(err, ErrObjectNotFound) {
			return err
		}
		if err := b.metadata.AbortCreate(ctx, b.namespace.ID, node.Inode); err != nil {
			return err
		}
	}
	deleted, err := b.metadata.ListState(ctx, b.namespace.ID, NodeStateDeleted)
	if err != nil {
		return err
	}
	for _, node := range deleted {
		if b.openCount(node.Inode) == 0 {
			if err := b.purgeDeleted(ctx, node); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Backend) GetNode(ctx context.Context, inode uint64) (Node, error) {
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err != nil {
		return Node{}, err
	}
	if !b.validNode(node, false) {
		return Node{}, ErrNotFound
	}
	return node, nil
}

func (b *Backend) Lookup(ctx context.Context, parent uint64, name string) (Node, error) {
	node, err := b.metadata.Lookup(ctx, b.namespace.ID, parent, name)
	if err != nil {
		return Node{}, err
	}
	if !b.validNode(node, false) {
		return Node{}, ErrNotFound
	}
	return node, nil
}

func (b *Backend) List(ctx context.Context, parent uint64) ([]Node, error) {
	nodes, err := b.metadata.List(ctx, b.namespace.ID, parent)
	if err != nil {
		return nil, err
	}
	result := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		if b.validNode(node, false) && node.Parent == parent {
			result = append(result, node)
		}
	}
	return result, nil
}

func (b *Backend) validNode(node Node, allowDeleted bool) bool {
	if node.NamespaceID != b.namespace.ID || node.Inode == 0 {
		return false
	}
	if node.State == NodeStateReady {
		return true
	}
	return allowDeleted && node.State == NodeStateDeleted
}

// ReadAt returns a plaintext range without materializing the full object.
func (b *Backend) ReadAt(ctx context.Context, node Node, offset int64, length int) ([]byte, error) {
	if !b.validNode(node, true) || !node.IsFile() || node.Generation <= 0 || node.ObjectKey == "" {
		return nil, ErrNotFound
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("dofs: invalid read range")
	}
	if length == 0 || offset >= node.Size {
		return []byte{}, nil
	}
	wanted := int64(length)
	if remaining := node.Size - offset; wanted > remaining {
		wanted = remaining
	}
	key, chunkSize, err := b.fileKeyAndChunkSize(ctx, node)
	if err != nil {
		return nil, err
	}
	defer Clear(key)
	plainEnd := offset + wanted - 1
	chunk := int64(chunkSize)
	startChunk := offset / chunk
	endChunk := plainEnd / chunk
	cipherChunkSize := chunk + NonceSize + TagSize
	cipherStart := int64(EncryptedHeaderSize) + startChunk*cipherChunkSize
	cipherEnd := int64(EncryptedHeaderSize) + (endChunk+1)*cipherChunkSize - 1
	if maximum := EncryptedSize(node.Size, chunk) - 1; cipherEnd > maximum {
		cipherEnd = maximum
	}
	reader, err := b.objects.GetRange(ctx, node.ObjectKey, cipherStart, cipherEnd)
	if err != nil {
		return nil, fmt.Errorf("read encrypted object range: %w", err)
	}
	defer reader.Close()
	trimStart := offset - startChunk*chunk
	trimEnd := plainEnd - startChunk*chunk
	var plaintext bytes.Buffer
	plaintext.Grow(int(wanted))
	if err := DecryptRange(key, reader, &plaintext, chunkSize, uint64(startChunk), trimStart, trimEnd); err != nil {
		return nil, fmt.Errorf("decrypt object range: %w", err)
	}
	return plaintext.Bytes(), nil
}

func (b *Backend) OpenPlaintext(ctx context.Context, node Node) (io.ReadCloser, error) {
	if !b.validNode(node, true) || !node.IsFile() || node.Generation <= 0 || node.ObjectKey == "" {
		return nil, ErrNotFound
	}
	key, chunkSize, err := b.fileKeyAndChunkSize(ctx, node)
	if err != nil {
		return nil, err
	}
	object, err := b.objects.GetRange(ctx, node.ObjectKey, 0, EncryptedSize(node.Size, int64(chunkSize))-1)
	if err != nil {
		Clear(key)
		return nil, fmt.Errorf("open encrypted object: %w", err)
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		err := DecryptStream(key, object, pipeWriter)
		Clear(key)
		closeErr := object.Close()
		if err == nil {
			err = closeErr
		}
		_ = pipeWriter.CloseWithError(err)
	}()
	return pipeReader, nil
}

func (b *Backend) fileKeyAndChunkSize(ctx context.Context, node Node) ([]byte, int, error) {
	if len(node.WrappedDEK) == 0 {
		return nil, 0, errors.New("dofs: file has no wrapped data key")
	}
	b.keyMu.Lock()
	cached := b.fileKeys[node.Inode]
	if cached != nil && cached.generation == node.Generation && bytes.Equal(cached.wrapper, node.WrappedDEK) && cached.chunkSize > 0 {
		key := append([]byte(nil), cached.key...)
		chunkSize := cached.chunkSize
		b.keyMu.Unlock()
		return key, chunkSize, nil
	}
	kek := append([]byte(nil), b.kek...)
	b.keyMu.Unlock()
	key, err := UnwrapKey(kek, node.WrappedDEK)
	Clear(kek)
	if err != nil {
		return nil, 0, fmt.Errorf("unwrap inode %d data key: %w", node.Inode, err)
	}
	header, err := b.objects.GetRange(ctx, node.ObjectKey, 0, EncryptedHeaderSize-1)
	if err != nil {
		Clear(key)
		return nil, 0, fmt.Errorf("read inode %d encrypted header: %w", node.Inode, err)
	}
	chunkSize, headerErr := ReadHeader(header)
	closeErr := header.Close()
	if headerErr != nil {
		Clear(key)
		return nil, 0, headerErr
	}
	if closeErr != nil {
		Clear(key)
		return nil, 0, closeErr
	}
	b.keyMu.Lock()
	if previous := b.fileKeys[node.Inode]; previous != nil {
		Clear(previous.key)
		Clear(previous.wrapper)
	}
	b.fileKeys[node.Inode] = &cachedFileKey{
		wrapper: append([]byte(nil), node.WrappedDEK...), key: append([]byte(nil), key...),
		generation: node.Generation, chunkSize: chunkSize,
	}
	b.keyMu.Unlock()
	return key, chunkSize, nil
}

func (b *Backend) requireMutation() error {
	if !b.CanMutate() {
		return ErrReadOnly
	}
	if !b.Writable() {
		return nil
	}
	select {
	case <-b.LeaseLost():
		if err := b.LeaseError(); err != nil {
			return err
		}
		return ErrWriterBusy
	default:
		return nil
	}
}

func (b *Backend) requireWriter() error {
	if !b.Writable() {
		return ErrReadOnly
	}
	return b.requireMutation()
}

func (b *Backend) CreateDirectory(ctx context.Context, parent uint64, name string, mode uint32) (Node, error) {
	if err := b.requireMutation(); err != nil {
		return Node{}, err
	}
	return b.metadata.CreateNode(ctx, CreateNodeRequest{
		NamespaceID: b.namespace.ID, Parent: parent, Name: name, Kind: NodeKindDirectory, Mode: mode,
	})
}

func (b *Backend) CreateFile(ctx context.Context, parent uint64, name string, mode uint32) (Node, error) {
	if err := b.requireMutation(); err != nil {
		return Node{}, err
	}
	dek, err := GenerateKey()
	if err != nil {
		return Node{}, err
	}
	defer Clear(dek)
	b.keyMu.Lock()
	kek := append([]byte(nil), b.kek...)
	b.keyMu.Unlock()
	wrapped, err := WrapKey(kek, dek)
	Clear(kek)
	if err != nil {
		return Node{}, fmt.Errorf("wrap new file key: %w", err)
	}
	node, err := b.metadata.CreateNode(ctx, CreateNodeRequest{
		NamespaceID: b.namespace.ID, Parent: parent, Name: name, Kind: NodeKindFile,
		Mode: mode, WrappedDEK: wrapped,
	})
	if err != nil {
		return Node{}, err
	}
	abort := true
	defer func() {
		if abort {
			_ = b.metadata.AbortCreate(context.Background(), b.namespace.ID, node.Inode)
		}
	}()
	transactionID, err := randomTransactionID()
	if err != nil {
		return Node{}, err
	}
	objectKey, err := GenerationObjectKey(b.namespace.ID, node.Inode, 1, transactionID)
	if err != nil {
		return Node{}, err
	}
	var encrypted bytes.Buffer
	if err := EncryptStream(dek, bytes.NewReader(nil), &encrypted); err != nil {
		return Node{}, err
	}
	if err := b.objects.Put(ctx, objectKey, bytes.NewReader(encrypted.Bytes()), int64(encrypted.Len())); err != nil {
		_ = b.objects.Delete(context.Background(), objectKey)
		return Node{}, fmt.Errorf("publish empty encrypted generation: %w", err)
	}
	committed, ok, err := b.metadata.CommitGeneration(ctx, CommitGenerationRequest{
		NamespaceID: b.namespace.ID, Inode: node.Inode, ExpectedGeneration: 0,
		ObjectKey: objectKey, Size: 0,
	})
	if err != nil || !ok {
		_ = b.objects.Delete(context.Background(), objectKey)
		if err != nil {
			return Node{}, err
		}
		return Node{}, ErrConflict
	}
	abort = false
	return committed, nil
}

func (b *Backend) Chmod(ctx context.Context, inode uint64, mode uint32) (Node, error) {
	if err := b.requireMutation(); err != nil {
		return Node{}, err
	}
	return b.metadata.Chmod(ctx, b.namespace.ID, inode, mode)
}

func (b *Backend) Rename(ctx context.Context, inode, newParent uint64, newName string, noReplace bool) (Node, error) {
	if err := b.requireMutation(); err != nil {
		return Node{}, err
	}
	renamed, replaced, err := b.metadata.Rename(ctx, RenameRequest{
		NamespaceID: b.namespace.ID, Inode: inode, NewParent: newParent,
		NewName: newName, NoReplace: noReplace,
	})
	if err != nil {
		return Node{}, err
	}
	// Only the backend holding the writeback lease can prove that no FUSE
	// handle still references a replaced inode. Control-plane mutations leave
	// tombstones for that writer (or mount recovery) to collect safely.
	if replaced != nil && b.Writable() && b.openCount(replaced.Inode) == 0 {
		_ = b.purgeDeleted(context.Background(), *replaced)
	}
	return renamed, nil
}

func (b *Backend) Remove(ctx context.Context, parent uint64, name string, directory bool) error {
	if err := b.requireMutation(); err != nil {
		return err
	}
	removed, err := b.metadata.Remove(ctx, RemoveRequest{
		NamespaceID: b.namespace.ID, Parent: parent, Name: name, Directory: directory,
	})
	if err != nil {
		return err
	}
	if b.Writable() && b.openCount(removed.Inode) == 0 {
		_ = b.purgeDeleted(context.Background(), removed)
	}
	return nil
}

func (b *Backend) retain(inode uint64) error {
	b.handleMu.Lock()
	defer b.handleMu.Unlock()
	if b.closed {
		return errors.New("dofs: backend is closed")
	}
	b.open[inode]++
	return nil
}

func (b *Backend) release(ctx context.Context, inode uint64) error {
	b.handleMu.Lock()
	if b.open[inode] > 1 {
		b.open[inode]--
		b.handleMu.Unlock()
		return nil
	}
	delete(b.open, inode)
	b.handleMu.Unlock()
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err == nil && node.State == NodeStateDeleted {
		return b.purgeDeleted(ctx, node)
	}
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (b *Backend) openCount(inode uint64) int {
	b.handleMu.Lock()
	defer b.handleMu.Unlock()
	return b.open[inode]
}

func (b *Backend) purgeDeleted(ctx context.Context, node Node) error {
	if node.State != NodeStateDeleted {
		return nil
	}
	generations, err := b.metadata.ListGenerations(ctx, b.namespace.ID, node.Inode)
	if err != nil {
		return err
	}
	for _, generation := range generations {
		if err := b.objects.Delete(ctx, generation.ObjectKey); err != nil && !errors.Is(err, ErrObjectNotFound) {
			return err
		}
	}
	_, err = b.metadata.Purge(ctx, b.namespace.ID, node.Inode)
	return err
}

func randomTransactionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

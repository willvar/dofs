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

const DefaultMountedReclaimInterval = 30 * time.Second

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
	// Mounted registers this backend as a live FUSE consumer. All mounted
	// backends, including read-only mounts, exclude physical reclamation while
	// Unix file handles may still reference immutable generations.
	Mounted bool
	// MountedReclaimInterval controls how often a mounted backend drains work
	// that external reclaimers must defer to its local open-handle registry.
	// Zero selects DefaultMountedReclaimInterval.
	MountedReclaimInterval time.Duration
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
	namespace    Namespace
	metadata     MetadataStore
	objects      ObjectStore
	keys         KeyProvider
	kek          []byte
	lease        Lease
	mountLease   Lease
	mountMu      sync.Mutex
	reclaimStop  context.CancelFunc
	reclaimDone  chan struct{}
	reclaimEvery time.Duration
	writeback    *WritebackManager
	writebackMu  sync.Mutex
	mutations    bool

	keyMu    sync.Mutex
	fileKeys map[uint64]*cachedFileKey

	handleMu sync.Mutex
	open     map[uint64]int
	closed   bool

	leaseStateMu  sync.Mutex
	leaseLost     chan struct{}
	leaseStop     chan struct{}
	leaseLostOnce sync.Once
	leaseStopOnce sync.Once
	leaseErr      error
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
	if options.MountedReclaimInterval < 0 {
		return nil, errors.New("dofs: mounted reclaim interval cannot be negative")
	}
	reclaimInterval := options.MountedReclaimInterval
	if reclaimInterval == 0 {
		reclaimInterval = DefaultMountedReclaimInterval
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
		mutations: options.Writable || options.ControlPlane, reclaimEvery: reclaimInterval,
	}
	if options.Mounted || options.Writable {
		if err := backend.protectMount(ctx); err != nil {
			backend.Close()
			return nil, err
		}
	}
	if options.Writable {
		lease, err := metadata.AcquireWriterLease(ctx, namespaceID)
		if err != nil {
			backend.Close()
			return nil, fmt.Errorf("acquire DOFS writer lease: %w", err)
		}
		backend.lease = lease
		backend.watchLease("writer", lease)
	}
	return backend, nil
}

func (b *Backend) protectMount(ctx context.Context) error {
	if b == nil {
		return errors.New("dofs: backend is required")
	}
	b.mountMu.Lock()
	defer b.mountMu.Unlock()
	if b.mountLease != nil {
		return nil
	}
	mountLease, err := b.metadata.AcquireMountLease(ctx, b.namespace.ID)
	if err != nil {
		return fmt.Errorf("acquire DOFS mount lease: %w", err)
	}
	b.mountLease = mountLease
	b.watchLease("mount", mountLease)
	b.startMountedReclaimerLocked()
	return nil
}

func (b *Backend) startMountedReclaimerLocked() {
	if b.reclaimStop != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.reclaimStop = cancel
	b.reclaimDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(b.reclaimEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				passCtx, passCancel := context.WithTimeout(ctx, 30*time.Second)
				_ = b.reclaimMounted(passCtx)
				passCancel()
			}
		}
	}()
}

func (b *Backend) Namespace() Namespace { return b.namespace }
func (b *Backend) Writable() bool       { return b != nil && b.lease != nil }
func (b *Backend) CanMutate() bool      { return b != nil && b.mutations }

func (b *Backend) LeaseLost() <-chan struct{} {
	if b == nil {
		return nil
	}
	b.leaseStateMu.Lock()
	defer b.leaseStateMu.Unlock()
	return b.leaseLost
}

func (b *Backend) LeaseError() error {
	if b == nil {
		return nil
	}
	b.leaseStateMu.Lock()
	err := b.leaseErr
	b.leaseStateMu.Unlock()
	if err != nil {
		return err
	}
	if b.lease != nil && b.lease.Err() != nil {
		return b.lease.Err()
	}
	if b.mountLease != nil {
		return b.mountLease.Err()
	}
	return nil
}

func (b *Backend) watchLease(label string, lease Lease) {
	if lease == nil || lease.Lost() == nil {
		return
	}
	b.leaseStateMu.Lock()
	if b.leaseLost == nil {
		b.leaseLost = make(chan struct{})
		b.leaseStop = make(chan struct{})
	}
	lost := b.leaseLost
	stop := b.leaseStop
	b.leaseStateMu.Unlock()
	go func() {
		select {
		case <-lease.Lost():
			err := lease.Err()
			if err == nil {
				err = ErrLeaseLost
			}
			b.leaseStateMu.Lock()
			if b.leaseErr == nil {
				b.leaseErr = fmt.Errorf("DOFS %s lease lost: %w", label, err)
			}
			b.leaseStateMu.Unlock()
			b.leaseLostOnce.Do(func() { close(lost) })
		case <-stop:
		}
	}()
}

func (b *Backend) Close() error {
	if b == nil {
		return nil
	}
	b.mountMu.Lock()
	reclaimStop := b.reclaimStop
	reclaimDone := b.reclaimDone
	b.mountMu.Unlock()
	if reclaimStop != nil {
		// Cancel object-store I/O before waiting for the handle mutex: a mounted
		// reclamation pass may currently hold it while deleting an object.
		reclaimStop()
	}
	b.handleMu.Lock()
	if b.closed {
		b.handleMu.Unlock()
		return nil
	}
	b.closed = true
	b.handleMu.Unlock()
	if reclaimDone != nil {
		<-reclaimDone
	}
	b.mountMu.Lock()
	b.reclaimStop = nil
	b.reclaimDone = nil
	b.mountMu.Unlock()
	b.leaseStateMu.Lock()
	if b.leaseStop != nil {
		b.leaseStopOnce.Do(func() { close(b.leaseStop) })
	}
	b.leaseStateMu.Unlock()
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
	var closeErrors []error
	if b.lease != nil {
		closeErrors = append(closeErrors, b.lease.Close())
	}
	if b.mountLease != nil {
		closeErrors = append(closeErrors, b.mountLease.Close())
	}
	return errors.Join(closeErrors...)
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
	return b.reclaimMounted(ctx)
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
	b.handleMu.Lock()
	defer b.handleMu.Unlock()
	if b.closed {
		return Node{}, errors.New("dofs: backend is closed")
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
	if replaced != nil && b.Writable() && b.open[replaced.Inode] == 0 {
		_ = b.purgeDeleted(context.Background(), *replaced)
	}
	return renamed, nil
}

func (b *Backend) Remove(ctx context.Context, parent uint64, name string, directory bool) error {
	if err := b.requireMutation(); err != nil {
		return err
	}
	b.handleMu.Lock()
	defer b.handleMu.Unlock()
	if b.closed {
		return errors.New("dofs: backend is closed")
	}
	removed, err := b.metadata.Remove(ctx, RemoveRequest{
		NamespaceID: b.namespace.ID, Parent: parent, Name: name, Directory: directory,
	})
	if err != nil {
		return err
	}
	if b.Writable() && b.open[removed.Inode] == 0 {
		_ = b.purgeDeleted(context.Background(), removed)
	}
	return nil
}

func (b *Backend) retain(ctx context.Context, inode uint64) error {
	b.handleMu.Lock()
	defer b.handleMu.Unlock()
	if b.closed {
		return errors.New("dofs: backend is closed")
	}
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err != nil {
		return err
	}
	if node.State != NodeStateReady && !(node.State == NodeStateDeleted && b.open[inode] > 0) {
		return ErrNotFound
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
	defer b.handleMu.Unlock()
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err == nil && node.State == NodeStateDeleted {
		return b.purgeDeleted(ctx, node)
	}
	if err == nil && node.State == NodeStateReady && node.IsFile() {
		return b.pruneSuperseded(ctx, node)
	}
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// reclaimMounted performs the reclamation work that the namespace's external
// collector must defer while this backend holds the exclusive mount lease.
// Each inode is checked against the local handle registry under the same lock
// used by Open, Unlink and replacement Rename.
func (b *Backend) reclaimMounted(ctx context.Context) error {
	if b.mountLease == nil {
		return nil
	}
	if err := requireLiveLease(b.mountLease); err != nil {
		return err
	}
	deleted, err := b.metadata.ListState(ctx, b.namespace.ID, NodeStateDeleted)
	if err != nil {
		return err
	}
	ready, readyErr := b.metadata.ListState(ctx, b.namespace.ID, NodeStateReady)
	if readyErr != nil {
		return readyErr
	}
	var reclaimErrors []error
	for _, node := range deleted {
		if err := b.reclaimNodeIfClosed(ctx, node); err != nil {
			reclaimErrors = append(reclaimErrors, fmt.Errorf("reclaim inode %d: %w", node.Inode, err))
		}
	}
	for _, node := range ready {
		if !node.IsFile() || node.Generation <= 1 {
			continue
		}
		if err := b.reclaimNodeIfClosed(ctx, node); err != nil {
			reclaimErrors = append(reclaimErrors, fmt.Errorf("prune inode %d generations: %w", node.Inode, err))
		}
	}
	return errors.Join(reclaimErrors...)
}

func (b *Backend) reclaimNodeIfClosed(ctx context.Context, node Node) error {
	b.handleMu.Lock()
	defer b.handleMu.Unlock()
	if b.closed || b.open[node.Inode] != 0 {
		return nil
	}
	if err := requireLiveLease(b.mountLease); err != nil {
		return err
	}
	switch node.State {
	case NodeStateDeleted:
		return b.purgeDeleted(ctx, node)
	case NodeStateReady:
		return b.pruneSuperseded(ctx, node)
	default:
		return nil
	}
}

func (b *Backend) purgeDeleted(ctx context.Context, node Node) error {
	if node.State != NodeStateDeleted {
		return nil
	}
	generations, err := b.metadata.ListGenerations(ctx, b.namespace.ID, node.Inode)
	if err != nil {
		return err
	}
	activeUploads, err := b.metadata.ListExternalUploads(ctx, b.namespace.ID, ExternalUploadActive)
	if err != nil {
		return err
	}
	committedUploads, err := b.metadata.ListExternalUploads(ctx, b.namespace.ID, ExternalUploadCommitted)
	if err != nil {
		return err
	}
	keys := make(map[string]struct{}, len(generations))
	for _, generation := range generations {
		keys[generation.ObjectKey] = struct{}{}
	}
	for _, upload := range append(activeUploads, committedUploads...) {
		if upload.Inode != node.Inode {
			continue
		}
		if upload.State == ExternalUploadActive && time.Now().UTC().Before(upload.ExpiresAt) {
			return nil
		}
		keys[upload.ObjectKey] = struct{}{}
	}
	for key := range keys {
		if err := b.objects.Delete(ctx, key); err != nil && !errors.Is(err, ErrObjectNotFound) {
			return err
		}
	}
	_, err = b.metadata.Purge(ctx, b.namespace.ID, node.Inode, node.Generation)
	return err
}

func (b *Backend) pruneSuperseded(ctx context.Context, node Node) error {
	if node.State != NodeStateReady || !node.IsFile() || node.Generation <= 1 {
		return nil
	}
	generations, err := b.metadata.ListGenerations(ctx, b.namespace.ID, node.Inode)
	if err != nil {
		return err
	}
	var currentCreatedAt time.Time
	for _, generation := range generations {
		if generation.Number == node.Generation {
			currentCreatedAt = generation.CreatedAt
			break
		}
	}
	if currentCreatedAt.IsZero() {
		return fmt.Errorf("dofs: inode %d current generation %d is missing", node.Inode, node.Generation)
	}
	if time.Now().UTC().Before(currentCreatedAt.Add(DefaultSupersededGenerationGrace)) {
		return nil
	}
	for _, generation := range generations {
		if generation.Number >= node.Generation {
			continue
		}
		if err := b.objects.Delete(ctx, generation.ObjectKey); err != nil && !errors.Is(err, ErrObjectNotFound) {
			return err
		}
		if _, err := b.metadata.DeleteGeneration(ctx, b.namespace.ID, node.Inode, generation.Number); err != nil {
			return err
		}
	}
	return nil
}

func randomTransactionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

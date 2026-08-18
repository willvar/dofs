package memory

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/willvar/dofs"
)

type namespaceState struct {
	namespace   dofs.Namespace
	nodes       map[uint64]dofs.Node
	children    map[uint64]map[string]uint64
	generations map[uint64][]dofs.Generation
	uploads     map[string]dofs.ExternalUpload
	events      []dofs.Event
	nextInode   uint64
	writerHeld  bool
}

// Store is a concurrency-safe implementation used by contract tests and
// single-process embedding. It deliberately implements the same atomic
// semantics as the PostgreSQL backend, but it is not durable.
type Store struct {
	mu         sync.Mutex
	namespaces map[string]*namespaceState
	nextEvent  int64
	now        func() time.Time
}

func New() *Store {
	return &Store{
		namespaces: make(map[string]*namespaceState),
		nextEvent:  1,
		now:        func() time.Time { return time.Now().UTC() },
	}
}

func cloneBytes(value []byte) []byte { return append([]byte(nil), value...) }

func cloneNamespace(value dofs.Namespace) dofs.Namespace {
	value.WrappedKEK = cloneBytes(value.WrappedKEK)
	return value
}

func cloneNode(value dofs.Node) dofs.Node {
	value.WrappedDEK = cloneBytes(value.WrappedDEK)
	if value.DeletedAt != nil {
		deleted := *value.DeletedAt
		value.DeletedAt = &deleted
	}
	return value
}

func cloneExternalUpload(value dofs.ExternalUpload) dofs.ExternalUpload {
	value.WrappedDEK = cloneBytes(value.WrappedDEK)
	if value.CommittedAt != nil {
		committed := *value.CommittedAt
		value.CommittedAt = &committed
	}
	return value
}

func (s *Store) CreateNamespace(ctx context.Context, namespace dofs.Namespace) (dofs.Namespace, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Namespace{}, err
	}
	if err := dofs.ValidateNamespaceID(namespace.ID); err != nil {
		return dofs.Namespace{}, err
	}
	if len(namespace.WrappedKEK) == 0 || namespace.QuotaBytes < 0 {
		return dofs.Namespace{}, dofs.ErrInvalidNamespace
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.namespaces[namespace.ID]; exists {
		return dofs.Namespace{}, dofs.ErrAlreadyExists
	}
	now := s.now()
	namespace.UsedBytes = 0
	namespace.CreatedAt = now
	namespace.UpdatedAt = now
	root := dofs.Node{
		NamespaceID: namespace.ID, Inode: dofs.RootInode, Kind: dofs.NodeKindDirectory,
		State: dofs.NodeStateReady, Mode: 0700, CreatedAt: now, ModifiedAt: now, ChangedAt: now,
	}
	s.namespaces[namespace.ID] = &namespaceState{
		namespace:   cloneNamespace(namespace),
		nodes:       map[uint64]dofs.Node{dofs.RootInode: root},
		children:    map[uint64]map[string]uint64{dofs.RootInode: {}},
		generations: make(map[uint64][]dofs.Generation),
		uploads:     make(map[string]dofs.ExternalUpload),
		nextInode:   dofs.RootInode + 1,
	}
	return cloneNamespace(namespace), nil
}

func (s *Store) GetNamespace(ctx context.Context, id string) (dofs.Namespace, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Namespace{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(id)
	if err != nil {
		return dofs.Namespace{}, err
	}
	return cloneNamespace(state.namespace), nil
}

func (s *Store) ListNamespaces(ctx context.Context) ([]dofs.Namespace, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]dofs.Namespace, 0, len(s.namespaces))
	for _, state := range s.namespaces {
		result = append(result, cloneNamespace(state.namespace))
	}
	sort.Slice(result, func(first, second int) bool { return result[first].ID < result[second].ID })
	return result, nil
}

func (s *Store) DeleteNamespace(ctx context.Context, id string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := dofs.ValidateNamespaceID(id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, exists := s.namespaces[id]
	if !exists {
		return dofs.ErrNotFound
	}
	if state.writerHeld {
		return dofs.ErrWriterBusy
	}
	delete(s.namespaces, id)
	return nil
}

func (s *Store) GetNode(ctx context.Context, namespaceID string, inode uint64) (dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.Node{}, err
	}
	node, exists := state.nodes[inode]
	if !exists {
		return dofs.Node{}, dofs.ErrNotFound
	}
	return cloneNode(node), nil
}

func (s *Store) Lookup(ctx context.Context, namespaceID string, parent uint64, name string) (dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, err
	}
	if err := dofs.ValidateName(name); err != nil {
		return dofs.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.Node{}, err
	}
	node, err := lookupReadyLocked(state, parent, name)
	return cloneNode(node), err
}

func (s *Store) List(ctx context.Context, namespaceID string, parent uint64) ([]dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return nil, err
	}
	if err := requireDirectoryLocked(state, parent); err != nil {
		return nil, err
	}
	result := make([]dofs.Node, 0, len(state.children[parent]))
	for _, inode := range state.children[parent] {
		node := state.nodes[inode]
		if node.State == dofs.NodeStateReady {
			result = append(result, cloneNode(node))
		}
	}
	sort.Slice(result, func(first, second int) bool {
		if result[first].IsDir() != result[second].IsDir() {
			return result[first].IsDir()
		}
		return result[first].Name < result[second].Name
	})
	return result, nil
}

func (s *Store) CreateNode(ctx context.Context, request dofs.CreateNodeRequest) (dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, err
	}
	if err := dofs.ValidateName(request.Name); err != nil {
		return dofs.Node{}, err
	}
	if request.Kind != dofs.NodeKindFile && request.Kind != dofs.NodeKindDirectory {
		return dofs.Node{}, dofs.ErrTypeMismatch
	}
	if request.Kind == dofs.NodeKindFile && len(request.WrappedDEK) == 0 {
		return dofs.Node{}, errors.New("dofs: file requires a wrapped data key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(request.NamespaceID)
	if err != nil {
		return dofs.Node{}, err
	}
	if err := requireDirectoryLocked(state, request.Parent); err != nil {
		return dofs.Node{}, err
	}
	if _, exists := state.children[request.Parent][request.Name]; exists {
		return dofs.Node{}, dofs.ErrAlreadyExists
	}
	now := s.now()
	mode := request.Mode & 0777
	if mode == 0 && request.Kind == dofs.NodeKindFile {
		mode = 0600
	}
	if mode == 0 && request.Kind == dofs.NodeKindDirectory {
		mode = 0700
	}
	stateValue := dofs.NodeStateCreating
	if request.Kind == dofs.NodeKindDirectory {
		stateValue = dofs.NodeStateReady
	}
	node := dofs.Node{
		NamespaceID: request.NamespaceID, Inode: state.nextInode, Parent: request.Parent,
		Name: request.Name, Kind: request.Kind, State: stateValue, Mode: mode,
		WrappedDEK: cloneBytes(request.WrappedDEK), CreatedAt: now, ModifiedAt: now, ChangedAt: now,
	}
	state.nextInode++
	state.nodes[node.Inode] = node
	state.children[request.Parent][request.Name] = node.Inode
	if node.IsDir() {
		state.children[node.Inode] = make(map[string]uint64)
		s.appendEventLocked(state, dofs.Event{NamespaceID: node.NamespaceID, Kind: dofs.EventCreate, Inode: node.Inode, Parent: node.Parent, Name: node.Name, CreatedAt: now})
	}
	return cloneNode(node), nil
}

func (s *Store) AbortCreate(ctx context.Context, namespaceID string, inode uint64) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return err
	}
	node, exists := state.nodes[inode]
	if !exists {
		return nil
	}
	if node.State != dofs.NodeStateCreating || node.Generation != 0 {
		return dofs.ErrConflict
	}
	delete(state.children[node.Parent], node.Name)
	delete(state.nodes, inode)
	return nil
}

func (s *Store) CommitGeneration(ctx context.Context, request dofs.CommitGenerationRequest) (dofs.Node, bool, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, false, err
	}
	if request.Inode == 0 || request.Size < 0 {
		return dofs.Node{}, false, errors.New("dofs: invalid generation")
	}
	if err := dofs.ValidateGenerationObjectKey(request.NamespaceID, request.Inode, request.ObjectKey); err != nil {
		return dofs.Node{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(request.NamespaceID)
	if err != nil {
		return dofs.Node{}, false, err
	}
	node, exists := state.nodes[request.Inode]
	if !exists || !node.IsFile() || (node.State != dofs.NodeStateCreating && node.State != dofs.NodeStateReady && node.State != dofs.NodeStateDeleted) {
		return dofs.Node{}, false, dofs.ErrNotFound
	}
	if node.Generation != request.ExpectedGeneration {
		return cloneNode(node), false, nil
	}
	delta := request.Size - node.Size
	if state.namespace.QuotaBytes > 0 && delta > 0 && state.namespace.UsedBytes+delta > state.namespace.QuotaBytes {
		return cloneNode(node), false, dofs.ErrQuotaExceeded
	}
	now := s.now()
	node.Generation++
	node.ObjectKey = request.ObjectKey
	node.Size = request.Size
	node.ModifiedAt = now
	node.ChangedAt = now
	wasCreating := node.State == dofs.NodeStateCreating
	if wasCreating {
		node.State = dofs.NodeStateReady
	}
	state.nodes[node.Inode] = node
	state.namespace.UsedBytes += delta
	state.namespace.UpdatedAt = now
	generation := dofs.Generation{
		NamespaceID: node.NamespaceID, Inode: node.Inode, Number: node.Generation,
		ObjectKey: node.ObjectKey, Size: node.Size, CreatedAt: now,
	}
	state.generations[node.Inode] = append(state.generations[node.Inode], generation)
	kind := dofs.EventWrite
	if wasCreating {
		kind = dofs.EventCreate
	}
	s.appendEventLocked(state, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: kind, Inode: node.Inode, Generation: node.Generation,
		Parent: node.Parent, Name: node.Name, CreatedAt: now,
	})
	return cloneNode(node), true, nil
}

func (s *Store) PrepareExternalUpload(ctx context.Context, request dofs.PrepareExternalUploadRequest) (dofs.ExternalUpload, error) {
	if err := contextError(ctx); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(request.ID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateName(request.Name); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if request.Parent == 0 || request.Size < 0 || len(request.WrappedDEK) == 0 || request.ExpiresAt.IsZero() {
		return dofs.ExternalUpload{}, errors.New("dofs: invalid external upload reservation")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(request.NamespaceID)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := requireDirectoryLocked(state, request.Parent); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if _, exists := state.uploads[request.ID]; exists {
		return dofs.ExternalUpload{}, dofs.ErrAlreadyExists
	}
	now := s.now()
	if !request.ExpiresAt.After(now) {
		return dofs.ExternalUpload{}, errors.New("dofs: external upload expiry must be in the future")
	}

	var node dofs.Node
	createsNode := false
	if inode, exists := state.children[request.Parent][request.Name]; exists {
		node = state.nodes[inode]
		if !request.Replace {
			return dofs.ExternalUpload{}, dofs.ErrAlreadyExists
		}
		if !node.IsFile() {
			return dofs.ExternalUpload{}, dofs.ErrTypeMismatch
		}
		if node.State != dofs.NodeStateReady {
			return dofs.ExternalUpload{}, dofs.ErrConflict
		}
		for _, upload := range state.uploads {
			if upload.Inode == node.Inode && upload.State == dofs.ExternalUploadActive {
				return dofs.ExternalUpload{}, dofs.ErrConflict
			}
		}
	} else {
		mode := request.Mode & 0777
		if mode == 0 {
			mode = 0600
		}
		node = dofs.Node{
			NamespaceID: request.NamespaceID, Inode: state.nextInode, Parent: request.Parent,
			Name: request.Name, Kind: dofs.NodeKindFile, State: dofs.NodeStateCreating,
			Mode: mode, WrappedDEK: cloneBytes(request.WrappedDEK),
			CreatedAt: now, ModifiedAt: now, ChangedAt: now,
		}
		createsNode = true
	}
	if node.Generation == math.MaxInt64 {
		return dofs.ExternalUpload{}, errors.New("dofs: generation space exhausted")
	}
	targetGeneration := node.Generation + 1
	objectKey, err := dofs.GenerationObjectKey(request.NamespaceID, node.Inode, targetGeneration, request.ID)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	if createsNode {
		state.nextInode++
		state.nodes[node.Inode] = node
		state.children[request.Parent][request.Name] = node.Inode
	}
	upload := dofs.ExternalUpload{
		ID: request.ID, NamespaceID: request.NamespaceID, Inode: node.Inode,
		ExpectedGeneration: node.Generation, TargetGeneration: targetGeneration,
		ObjectKey: objectKey, Size: request.Size, WrappedDEK: cloneBytes(request.WrappedDEK),
		CreatesNode: createsNode, State: dofs.ExternalUploadActive,
		ExpiresAt: request.ExpiresAt.UTC(), CreatedAt: now,
	}
	state.uploads[upload.ID] = upload
	return cloneExternalUpload(upload), nil
}

func (s *Store) GetExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.ExternalUpload, error) {
	if err := contextError(ctx); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	upload, exists := state.uploads[uploadID]
	if !exists {
		return dofs.ExternalUpload{}, dofs.ErrNotFound
	}
	return cloneExternalUpload(upload), nil
}

func (s *Store) ListExternalUploads(ctx context.Context, namespaceID string, uploadState dofs.ExternalUploadState) ([]dofs.ExternalUpload, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if err := dofs.ValidateExternalUploadState(uploadState); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return nil, err
	}
	result := make([]dofs.ExternalUpload, 0)
	for _, upload := range state.uploads {
		if upload.State == uploadState {
			result = append(result, cloneExternalUpload(upload))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (s *Store) RefreshExternalUpload(ctx context.Context, namespaceID, uploadID string, expiresAt time.Time) (dofs.ExternalUpload, error) {
	if err := contextError(ctx); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	upload, exists := state.uploads[uploadID]
	if !exists {
		return dofs.ExternalUpload{}, dofs.ErrNotFound
	}
	now := s.now()
	if upload.State != dofs.ExternalUploadActive || !upload.ExpiresAt.After(now) || !expiresAt.After(now) {
		return cloneExternalUpload(upload), dofs.ErrConflict
	}
	upload.ExpiresAt = expiresAt.UTC()
	state.uploads[uploadID] = upload
	return cloneExternalUpload(upload), nil
}

func (s *Store) CommitExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.Node, bool, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, false, err
	}
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.Node{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.Node{}, false, err
	}
	upload, exists := state.uploads[uploadID]
	if !exists {
		return dofs.Node{}, false, dofs.ErrNotFound
	}
	node, exists := state.nodes[upload.Inode]
	if !exists || !node.IsFile() {
		return dofs.Node{}, false, dofs.ErrNotFound
	}
	if upload.State == dofs.ExternalUploadCommitted {
		if node.Generation == upload.TargetGeneration && node.ObjectKey == upload.ObjectKey &&
			node.Size == upload.Size && bytesEqual(node.WrappedDEK, upload.WrappedDEK) {
			return cloneNode(node), false, nil
		}
		return cloneNode(node), false, dofs.ErrConflict
	}
	if upload.State != dofs.ExternalUploadActive || !upload.ExpiresAt.After(s.now()) || node.Generation != upload.ExpectedGeneration {
		return cloneNode(node), false, dofs.ErrConflict
	}
	if upload.CreatesNode {
		if node.State != dofs.NodeStateCreating {
			return cloneNode(node), false, dofs.ErrConflict
		}
	} else if node.State != dofs.NodeStateReady {
		return cloneNode(node), false, dofs.ErrConflict
	}
	delta := upload.Size - node.Size
	if delta > 0 && (state.namespace.UsedBytes > math.MaxInt64-delta ||
		(state.namespace.QuotaBytes > 0 && delta > state.namespace.QuotaBytes-state.namespace.UsedBytes)) {
		return cloneNode(node), false, dofs.ErrQuotaExceeded
	}
	now := s.now()
	node.Generation = upload.TargetGeneration
	node.ObjectKey = upload.ObjectKey
	node.Size = upload.Size
	node.WrappedDEK = cloneBytes(upload.WrappedDEK)
	node.ModifiedAt = now
	node.ChangedAt = now
	if upload.CreatesNode {
		node.State = dofs.NodeStateReady
	}
	state.nodes[node.Inode] = node
	state.namespace.UsedBytes += delta
	state.namespace.UpdatedAt = now
	state.generations[node.Inode] = append(state.generations[node.Inode], dofs.Generation{
		NamespaceID: node.NamespaceID, Inode: node.Inode, Number: node.Generation,
		ObjectKey: node.ObjectKey, Size: node.Size, CreatedAt: now,
	})
	upload.State = dofs.ExternalUploadCommitted
	upload.CommittedAt = &now
	state.uploads[upload.ID] = upload
	kind := dofs.EventWrite
	if upload.CreatesNode {
		kind = dofs.EventCreate
	}
	s.appendEventLocked(state, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: kind, Inode: node.Inode, Generation: node.Generation,
		Parent: node.Parent, Name: node.Name, CreatedAt: now,
	})
	return cloneNode(node), true, nil
}

func (s *Store) AbortExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.ExternalUpload, error) {
	if err := contextError(ctx); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	upload, exists := state.uploads[uploadID]
	if !exists {
		return dofs.ExternalUpload{}, dofs.ErrNotFound
	}
	if upload.State != dofs.ExternalUploadActive {
		return cloneExternalUpload(upload), dofs.ErrConflict
	}
	if upload.CreatesNode {
		node, exists := state.nodes[upload.Inode]
		if !exists || node.State != dofs.NodeStateCreating || node.Generation != 0 {
			return cloneExternalUpload(upload), dofs.ErrConflict
		}
		delete(state.children[node.Parent], node.Name)
		delete(state.nodes, node.Inode)
	}
	delete(state.uploads, uploadID)
	return cloneExternalUpload(upload), nil
}

func (s *Store) AcknowledgeExternalUpload(ctx context.Context, namespaceID, uploadID string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return err
	}
	upload, exists := state.uploads[uploadID]
	if !exists {
		return dofs.ErrNotFound
	}
	if upload.State != dofs.ExternalUploadCommitted {
		return dofs.ErrConflict
	}
	delete(state.uploads, uploadID)
	return nil
}

func bytesEqual(first, second []byte) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}

func (s *Store) Rename(ctx context.Context, request dofs.RenameRequest) (dofs.Node, *dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, nil, err
	}
	if err := dofs.ValidateName(request.NewName); err != nil {
		return dofs.Node{}, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(request.NamespaceID)
	if err != nil {
		return dofs.Node{}, nil, err
	}
	node, exists := state.nodes[request.Inode]
	if !exists || node.State != dofs.NodeStateReady || node.Inode == dofs.RootInode {
		return dofs.Node{}, nil, dofs.ErrNotFound
	}
	if err := requireDirectoryLocked(state, request.NewParent); err != nil {
		return dofs.Node{}, nil, err
	}
	if node.IsDir() && isDescendantLocked(state, request.NewParent, node.Inode) {
		return dofs.Node{}, nil, dofs.ErrInvalidName
	}
	if node.Parent == request.NewParent && node.Name == request.NewName {
		copy := cloneNode(node)
		return copy, nil, nil
	}
	var replaced *dofs.Node
	if destinationInode, found := state.children[request.NewParent][request.NewName]; found {
		destination := state.nodes[destinationInode]
		if destination.Inode == node.Inode {
			copy := cloneNode(node)
			return copy, nil, nil
		}
		if request.NoReplace {
			return dofs.Node{}, nil, dofs.ErrAlreadyExists
		}
		if destination.Kind != node.Kind {
			return dofs.Node{}, nil, dofs.ErrTypeMismatch
		}
		if destination.IsDir() && len(state.children[destination.Inode]) != 0 {
			return dofs.Node{}, nil, dofs.ErrDirectoryNotEmpty
		}
		now := s.now()
		oldDestinationParent, oldDestinationName := destination.Parent, destination.Name
		tombstoneLocked(state, &destination, now)
		state.nodes[destination.Inode] = destination
		s.appendEventLocked(state, dofs.Event{
			NamespaceID: destination.NamespaceID, Kind: dofs.EventRemove, Inode: destination.Inode,
			Generation: destination.Generation, OldParent: oldDestinationParent,
			OldName: oldDestinationName, CreatedAt: now,
		})
		copy := cloneNode(destination)
		replaced = &copy
	}
	oldParent, oldName := node.Parent, node.Name
	delete(state.children[oldParent], oldName)
	state.children[request.NewParent][request.NewName] = node.Inode
	now := s.now()
	node.Parent = request.NewParent
	node.Name = request.NewName
	node.ChangedAt = now
	state.nodes[node.Inode] = node
	s.appendEventLocked(state, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: dofs.EventRename, Inode: node.Inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name,
		OldParent: oldParent, OldName: oldName, CreatedAt: now,
	})
	return cloneNode(node), replaced, nil
}

func (s *Store) Remove(ctx context.Context, request dofs.RemoveRequest) (dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, err
	}
	if err := dofs.ValidateName(request.Name); err != nil {
		return dofs.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(request.NamespaceID)
	if err != nil {
		return dofs.Node{}, err
	}
	node, err := lookupReadyLocked(state, request.Parent, request.Name)
	if err != nil {
		return dofs.Node{}, err
	}
	if node.IsDir() != request.Directory {
		return dofs.Node{}, dofs.ErrTypeMismatch
	}
	if node.IsDir() && len(state.children[node.Inode]) != 0 {
		return dofs.Node{}, dofs.ErrDirectoryNotEmpty
	}
	now := s.now()
	oldParent, oldName := node.Parent, node.Name
	tombstoneLocked(state, &node, now)
	state.nodes[node.Inode] = node
	s.appendEventLocked(state, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: dofs.EventRemove, Inode: node.Inode,
		Generation: node.Generation, OldParent: oldParent, OldName: oldName, CreatedAt: now,
	})
	return cloneNode(node), nil
}

func (s *Store) Purge(ctx context.Context, namespaceID string, inode uint64) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return false, err
	}
	node, exists := state.nodes[inode]
	if !exists {
		return false, nil
	}
	if node.State != dofs.NodeStateDeleted {
		return false, dofs.ErrConflict
	}
	if node.IsDir() && len(state.children[inode]) != 0 {
		return false, dofs.ErrDirectoryNotEmpty
	}
	state.namespace.UsedBytes -= node.Size
	state.namespace.UpdatedAt = s.now()
	delete(state.generations, inode)
	delete(state.children, inode)
	delete(state.nodes, inode)
	return true, nil
}

func (s *Store) ListGenerations(ctx context.Context, namespaceID string, inode uint64) ([]dofs.Generation, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return nil, err
	}
	if _, exists := state.nodes[inode]; !exists {
		return nil, dofs.ErrNotFound
	}
	values := state.generations[inode]
	result := append([]dofs.Generation(nil), values...)
	sort.Slice(result, func(first, second int) bool { return result[first].Number < result[second].Number })
	return result, nil
}

func (s *Store) ListState(ctx context.Context, namespaceID string, stateValue dofs.NodeState) ([]dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return nil, err
	}
	var result []dofs.Node
	for _, node := range state.nodes {
		if node.State == stateValue {
			result = append(result, cloneNode(node))
		}
	}
	sort.Slice(result, func(first, second int) bool { return result[first].Inode < result[second].Inode })
	return result, nil
}

func (s *Store) Chmod(ctx context.Context, namespaceID string, inode uint64, mode uint32) (dofs.Node, error) {
	if err := contextError(ctx); err != nil {
		return dofs.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return dofs.Node{}, err
	}
	node, exists := state.nodes[inode]
	if !exists || node.State != dofs.NodeStateReady {
		return dofs.Node{}, dofs.ErrNotFound
	}
	node.Mode = mode & 0777
	node.ChangedAt = s.now()
	state.nodes[inode] = node
	s.appendEventLocked(state, dofs.Event{
		NamespaceID: namespaceID, Kind: dofs.EventChmod, Inode: inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name, CreatedAt: node.ChangedAt,
	})
	return cloneNode(node), nil
}

func (s *Store) ListEvents(ctx context.Context, namespaceID string, after int64, limit int) ([]dofs.Event, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return nil, err
	}
	result := make([]dofs.Event, 0, limit)
	for _, event := range state.events {
		if event.Sequence > after {
			result = append(result, event)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func (s *Store) LatestEventSequence(ctx context.Context, namespaceID string) (int64, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return 0, err
	}
	if len(state.events) == 0 {
		return 0, nil
	}
	return state.events[len(state.events)-1].Sequence, nil
}

func (s *Store) AcquireWriterLease(ctx context.Context, namespaceID string) (dofs.Lease, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.namespaceLocked(namespaceID)
	if err != nil {
		return nil, err
	}
	if state.writerHeld {
		return nil, dofs.ErrWriterBusy
	}
	state.writerHeld = true
	return &lease{store: s, namespaceID: namespaceID}, nil
}

func (s *Store) namespaceLocked(id string) (*namespaceState, error) {
	state := s.namespaces[id]
	if state == nil {
		return nil, dofs.ErrNotFound
	}
	return state, nil
}

func requireDirectoryLocked(state *namespaceState, inode uint64) error {
	node, exists := state.nodes[inode]
	if !exists || node.State != dofs.NodeStateReady {
		return dofs.ErrNotFound
	}
	if !node.IsDir() {
		return dofs.ErrNotDirectory
	}
	return nil
}

func lookupReadyLocked(state *namespaceState, parent uint64, name string) (dofs.Node, error) {
	if err := requireDirectoryLocked(state, parent); err != nil {
		return dofs.Node{}, err
	}
	inode, exists := state.children[parent][name]
	if !exists {
		return dofs.Node{}, dofs.ErrNotFound
	}
	node := state.nodes[inode]
	if node.State != dofs.NodeStateReady {
		return dofs.Node{}, dofs.ErrNotFound
	}
	return node, nil
}

func isDescendantLocked(state *namespaceState, candidate, ancestor uint64) bool {
	for candidate != 0 {
		if candidate == ancestor {
			return true
		}
		node, exists := state.nodes[candidate]
		if !exists || node.Parent == candidate {
			return false
		}
		candidate = node.Parent
	}
	return false
}

func tombstoneLocked(state *namespaceState, node *dofs.Node, now time.Time) {
	delete(state.children[node.Parent], node.Name)
	node.Parent = 0
	node.State = dofs.NodeStateDeleted
	node.ChangedAt = now
	node.DeletedAt = &now
}

func (s *Store) appendEventLocked(state *namespaceState, event dofs.Event) {
	event.Sequence = s.nextEvent
	s.nextEvent++
	state.events = append(state.events, event)
}

func contextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type lease struct {
	store       *Store
	namespaceID string
	once        sync.Once
}

func (l *lease) Close() error {
	if l == nil || l.store == nil {
		return nil
	}
	l.once.Do(func() {
		l.store.mu.Lock()
		defer l.store.mu.Unlock()
		if state := l.store.namespaces[l.namespaceID]; state != nil {
			state.writerHeld = false
		}
	})
	return nil
}

func (*lease) Lost() <-chan struct{} { return nil }
func (*lease) Err() error            { return nil }

var _ dofs.MetadataStore = (*Store)(nil)

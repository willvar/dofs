package dofs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	RootInode uint64 = 1

	NodeStateCreating NodeState = "creating"
	NodeStateReady    NodeState = "ready"
	NodeStateDeleted  NodeState = "deleted"

	NodeKindFile      NodeKind = "file"
	NodeKindDirectory NodeKind = "directory"
)

var (
	ErrNotFound          = errors.New("dofs: node not found")
	ErrAlreadyExists     = errors.New("dofs: node already exists")
	ErrNotDirectory      = errors.New("dofs: node is not a directory")
	ErrIsDirectory       = errors.New("dofs: node is a directory")
	ErrDirectoryNotEmpty = errors.New("dofs: directory is not empty")
	ErrTypeMismatch      = errors.New("dofs: node type mismatch")
	ErrConflict          = errors.New("dofs: generation conflict")
	ErrQuotaExceeded     = errors.New("dofs: namespace quota exceeded")
	ErrWriterBusy        = errors.New("dofs: writable namespace already mounted")
	ErrInvalidName       = errors.New("dofs: invalid name")
	ErrInvalidNamespace  = errors.New("dofs: invalid namespace")
	ErrInvalidObjectKey  = errors.New("dofs: invalid object key")
	ErrObjectNotFound    = errors.New("dofs: object not found")
	ErrReadOnly          = errors.New("dofs: filesystem is read-only")
	ErrWriteConflict     = errors.New("dofs: generation changed during writeback")
	ErrHandleClosed      = errors.New("dofs: write handle is closed")
	ErrLeaseLost         = errors.New("dofs: writable namespace lease was lost")
)

type NodeState string
type NodeKind string

// Namespace is an independently encrypted filesystem and quota boundary.
// WrappedKEK contains a namespace key encrypted by the configured key provider;
// plaintext keys must never be persisted in metadata.
type Namespace struct {
	ID         string
	Label      string
	WrappedKEK []byte
	QuotaBytes int64
	UsedBytes  int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Node is the stable metadata representation of a directory entry. Parent is
// zero only for the root inode and detached tombstones. Paths are derived by
// walking parents and are deliberately absent from persisted identity.
type Node struct {
	NamespaceID string
	Inode       uint64
	Parent      uint64
	Name        string
	Kind        NodeKind
	State       NodeState
	Mode        uint32
	Size        int64
	Generation  int64
	ObjectKey   string
	WrappedDEK  []byte
	CreatedAt   time.Time
	ModifiedAt  time.Time
	ChangedAt   time.Time
	DeletedAt   *time.Time
}

func (n Node) IsDir() bool  { return n.Kind == NodeKindDirectory }
func (n Node) IsFile() bool { return n.Kind == NodeKindFile }

// Generation is an immutable encrypted representation of one file version.
// A generation is visible only after MetadataStore.CommitGeneration advances
// the node with a compare-and-swap transaction.
type Generation struct {
	NamespaceID string
	Inode       uint64
	Number      int64
	ObjectKey   string
	Size        int64
	CreatedAt   time.Time
}

type CreateNodeRequest struct {
	NamespaceID string
	Parent      uint64
	Name        string
	Kind        NodeKind
	Mode        uint32
	WrappedDEK  []byte
}

type CommitGenerationRequest struct {
	NamespaceID        string
	Inode              uint64
	ExpectedGeneration int64
	ObjectKey          string
	Size               int64
}

type ExternalUploadState string

const (
	ExternalUploadActive    ExternalUploadState = "active"
	ExternalUploadCommitted ExternalUploadState = "committed"
)

// ExternalUpload is a durable control-plane reservation for ciphertext that
// an application uploads directly to object storage. DOFS never proxies the
// payload: it allocates the inode/generation target, owns the wrapped file key,
// verifies the resulting object and atomically publishes the generation.
type ExternalUpload struct {
	ID                 string
	NamespaceID        string
	Inode              uint64
	ExpectedGeneration int64
	TargetGeneration   int64
	ObjectKey          string
	Size               int64
	WrappedDEK         []byte
	CreatesNode        bool
	State              ExternalUploadState
	ExpiresAt          time.Time
	CreatedAt          time.Time
	CommittedAt        *time.Time
}

type PrepareExternalUploadRequest struct {
	ID          string
	NamespaceID string
	Parent      uint64
	Name        string
	Mode        uint32
	Size        int64
	WrappedDEK  []byte
	Replace     bool
	ExpiresAt   time.Time
}

type RenameRequest struct {
	NamespaceID string
	Inode       uint64
	NewParent   uint64
	NewName     string
	NoReplace   bool
}

type RemoveRequest struct {
	NamespaceID string
	Parent      uint64
	Name        string
	Directory   bool
}

type EventKind string

const (
	EventCreate EventKind = "create"
	EventWrite  EventKind = "write"
	EventRename EventKind = "rename"
	EventRemove EventKind = "remove"
	EventChmod  EventKind = "chmod"
)

// Event is committed in the same metadata transaction as the change it
// describes. Sequence is monotonic within the metadata database and is the
// cursor used by UI/indexer consumers.
type Event struct {
	Sequence    int64
	NamespaceID string
	Kind        EventKind
	Inode       uint64
	Generation  int64
	Parent      uint64
	Name        string
	OldParent   uint64
	OldName     string
	CreatedAt   time.Time
}

// MetadataStore owns namespace, inode, generation, quota, event and writer
// lease semantics. Implementations must make every mutating method atomic.
type MetadataStore interface {
	CreateNamespace(context.Context, Namespace) (Namespace, error)
	GetNamespace(context.Context, string) (Namespace, error)
	ListNamespaces(context.Context) ([]Namespace, error)
	DeleteNamespace(context.Context, string) error

	GetNode(context.Context, string, uint64) (Node, error)
	Lookup(context.Context, string, uint64, string) (Node, error)
	List(context.Context, string, uint64) ([]Node, error)
	CreateNode(context.Context, CreateNodeRequest) (Node, error)
	AbortCreate(context.Context, string, uint64) error
	CommitGeneration(context.Context, CommitGenerationRequest) (Node, bool, error)
	Rename(context.Context, RenameRequest) (Node, *Node, error)
	Remove(context.Context, RemoveRequest) (Node, error)
	Purge(context.Context, string, uint64) (bool, error)
	ListGenerations(context.Context, string, uint64) ([]Generation, error)
	ListState(context.Context, string, NodeState) ([]Node, error)
	Chmod(context.Context, string, uint64, uint32) (Node, error)
	ListEvents(context.Context, string, int64, int) ([]Event, error)
	LatestEventSequence(context.Context, string) (int64, error)

	PrepareExternalUpload(context.Context, PrepareExternalUploadRequest) (ExternalUpload, error)
	GetExternalUpload(context.Context, string, string) (ExternalUpload, error)
	ListExternalUploads(context.Context, string, ExternalUploadState) ([]ExternalUpload, error)
	RefreshExternalUpload(context.Context, string, string, time.Time) (ExternalUpload, error)
	CommitExternalUpload(context.Context, string, string) (Node, bool, error)
	AbortExternalUpload(context.Context, string, string) (ExternalUpload, error)
	AcknowledgeExternalUpload(context.Context, string, string) error

	AcquireWriterLease(context.Context, string) (Lease, error)
}

// Lease remains valid until Lost closes. A nil Lost channel means that the
// implementation cannot actively report loss and only releases on Close.
type Lease interface {
	io.Closer
	Lost() <-chan struct{}
	Err() error
}

// ObjectStore contains encrypted immutable generations. Range end offsets are
// inclusive. Implementations must reject path traversal and ambiguous keys.
type ObjectStore interface {
	GetRange(context.Context, string, int64, int64) (io.ReadCloser, error)
	Stat(context.Context, string) (ObjectInfo, error)
	Put(context.Context, string, io.Reader, int64) error
	Copy(context.Context, string, string) error
	Delete(context.Context, string) error
	DeletePrefix(context.Context, string) error
}

type ObjectInfo struct {
	Size int64
}

// KeyProvider unwraps the persisted namespace KEK. Returned bytes are owned by
// the caller and must be cleared after use.
type KeyProvider interface {
	UnwrapNamespaceKey(context.Context, Namespace) ([]byte, error)
}

var namespaceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var externalUploadIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func ValidateNamespaceID(id string) error {
	if !namespaceIDPattern.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidNamespace, id)
	}
	return nil
}

func ValidateExternalUploadID(id string) error {
	if !externalUploadIDPattern.MatchString(id) {
		return fmt.Errorf("dofs: invalid external upload ID %q", id)
	}
	return nil
}

func ValidateExternalUploadState(state ExternalUploadState) error {
	if state != ExternalUploadActive && state != ExternalUploadCommitted {
		return fmt.Errorf("dofs: invalid external upload state %q", state)
	}
	return nil
}

func ValidateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	return nil
}

func ObjectRoot(namespaceID string) (string, error) {
	if err := ValidateNamespaceID(namespaceID); err != nil {
		return "", err
	}
	return path.Join(".dofs", "v1", "namespaces", namespaceID, "objects") + "/", nil
}

func InodeObjectRoot(namespaceID string, inode uint64) (string, error) {
	root, err := ObjectRoot(namespaceID)
	if err != nil {
		return "", err
	}
	if inode == 0 {
		return "", errors.New("dofs: inode must be greater than zero")
	}
	return path.Join(root, strconv.FormatUint(inode, 10)) + "/", nil
}

func GenerationObjectKey(namespaceID string, inode uint64, generation int64, transactionID string) (string, error) {
	root, err := InodeObjectRoot(namespaceID, inode)
	if err != nil {
		return "", err
	}
	if generation <= 0 {
		return "", errors.New("dofs: generation must be greater than zero")
	}
	if transactionID == "" || strings.ContainsAny(transactionID, "/\x00") {
		return "", errors.New("dofs: invalid transaction ID")
	}
	return path.Join(root, fmt.Sprintf("%020d-%s.dofs", generation, transactionID)), nil
}

// ValidateGenerationObjectKey prevents metadata in one namespace or inode from
// referencing (and later deleting) another inode's immutable object.
func ValidateGenerationObjectKey(namespaceID string, inode uint64, objectKey string) error {
	root, err := InodeObjectRoot(namespaceID, inode)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(objectKey, root) {
		return fmt.Errorf("%w: key is outside inode %d", ErrInvalidObjectKey, inode)
	}
	leaf := strings.TrimPrefix(objectKey, root)
	if leaf == "" || strings.ContainsAny(leaf, "/\x00") || path.Clean(objectKey) != objectKey {
		return fmt.Errorf("%w: malformed generation key", ErrInvalidObjectKey)
	}
	return nil
}

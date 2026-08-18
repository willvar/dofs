package dofs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultDirectUploadTTL = 2 * time.Hour
	maxDirectUploadTTL     = 24 * time.Hour
)

func directUploadExpiry(ttl time.Duration) (time.Time, error) {
	if ttl == 0 {
		ttl = defaultDirectUploadTTL
	}
	if ttl <= 0 || ttl > maxDirectUploadTTL {
		return time.Time{}, fmt.Errorf("dofs: direct upload TTL must be between 1ns and %s", maxDirectUploadTTL)
	}
	return time.Now().UTC().Add(ttl), nil
}

// DirectUploadRequest reserves a generation for ciphertext uploaded directly
// by an application or browser. FileKey is copied only long enough to wrap it;
// the caller retains ownership and should clear its buffer when possible.
type DirectUploadRequest struct {
	ID      string
	Parent  uint64
	Name    string
	Mode    uint32
	Size    int64
	FileKey []byte
	Replace bool
	TTL     time.Duration
}

func (b *Backend) PrepareDirectUpload(ctx context.Context, request DirectUploadRequest) (ExternalUpload, error) {
	if err := b.requireMutation(); err != nil {
		return ExternalUpload{}, err
	}
	if request.ID == "" {
		var err error
		request.ID, err = randomTransactionID()
		if err != nil {
			return ExternalUpload{}, fmt.Errorf("generate direct upload ID: %w", err)
		}
	}
	if err := ValidateExternalUploadID(request.ID); err != nil {
		return ExternalUpload{}, err
	}
	if err := ValidateName(request.Name); err != nil {
		return ExternalUpload{}, err
	}
	if request.Parent == 0 || request.Size < 0 || len(request.FileKey) != KeySize {
		return ExternalUpload{}, errors.New("dofs: invalid direct upload request")
	}
	expiresAt, err := directUploadExpiry(request.TTL)
	if err != nil {
		return ExternalUpload{}, err
	}
	b.keyMu.Lock()
	kek := append([]byte(nil), b.kek...)
	b.keyMu.Unlock()
	wrapped, err := WrapKey(kek, request.FileKey)
	Clear(kek)
	if err != nil {
		return ExternalUpload{}, fmt.Errorf("wrap direct upload file key: %w", err)
	}
	defer Clear(wrapped)
	upload, err := b.metadata.PrepareExternalUpload(ctx, PrepareExternalUploadRequest{
		ID: request.ID, NamespaceID: b.namespace.ID, Parent: request.Parent,
		Name: request.Name, Mode: request.Mode, Size: request.Size,
		WrappedDEK: wrapped, Replace: request.Replace, ExpiresAt: expiresAt,
	})
	if err != nil {
		return ExternalUpload{}, err
	}
	if err := b.validateExternalUpload(upload); err != nil {
		return ExternalUpload{}, err
	}
	return upload, nil
}

// PrepareCiphertextClone reserves a destination that uses the source inode's
// wrapped key and plaintext size. The caller may perform an object-store-side
// ciphertext copy from source.ObjectKey to the returned ObjectKey, avoiding
// plaintext and application-server bandwidth entirely.
func (b *Backend) PrepareCiphertextClone(
	ctx context.Context,
	uploadID string,
	parent uint64,
	name string,
	mode uint32,
	source Node,
	replace bool,
	ttl time.Duration,
) (ExternalUpload, error) {
	if err := b.requireMutation(); err != nil {
		return ExternalUpload{}, err
	}
	if !b.validNode(source, false) || !source.IsFile() || source.Generation <= 0 ||
		source.ObjectKey == "" || len(source.WrappedDEK) == 0 {
		return ExternalUpload{}, ErrNotFound
	}
	if uploadID == "" {
		var err error
		uploadID, err = randomTransactionID()
		if err != nil {
			return ExternalUpload{}, err
		}
	}
	if err := ValidateExternalUploadID(uploadID); err != nil {
		return ExternalUpload{}, err
	}
	if err := ValidateName(name); err != nil {
		return ExternalUpload{}, err
	}
	expiresAt, err := directUploadExpiry(ttl)
	if err != nil {
		return ExternalUpload{}, err
	}
	upload, err := b.metadata.PrepareExternalUpload(ctx, PrepareExternalUploadRequest{
		ID: uploadID, NamespaceID: b.namespace.ID, Parent: parent, Name: name,
		Mode: mode, Size: source.Size, WrappedDEK: source.WrappedDEK,
		Replace: replace, ExpiresAt: expiresAt,
	})
	if err != nil {
		return ExternalUpload{}, err
	}
	if err := b.validateExternalUpload(upload); err != nil {
		return ExternalUpload{}, err
	}
	return upload, nil
}

func (b *Backend) GetDirectUpload(ctx context.Context, uploadID string) (ExternalUpload, error) {
	if err := ValidateExternalUploadID(uploadID); err != nil {
		return ExternalUpload{}, err
	}
	upload, err := b.metadata.GetExternalUpload(ctx, b.namespace.ID, uploadID)
	if err != nil {
		return ExternalUpload{}, err
	}
	if err := b.validateExternalUpload(upload); err != nil {
		return ExternalUpload{}, err
	}
	return upload, nil
}

// RefreshDirectUpload extends a live reservation. Expired or already
// committed reservations cannot be revived.
func (b *Backend) RefreshDirectUpload(ctx context.Context, uploadID string, ttl time.Duration) (ExternalUpload, error) {
	if err := b.requireMutation(); err != nil {
		return ExternalUpload{}, err
	}
	if err := ValidateExternalUploadID(uploadID); err != nil {
		return ExternalUpload{}, err
	}
	expiresAt, err := directUploadExpiry(ttl)
	if err != nil {
		return ExternalUpload{}, err
	}
	upload, err := b.metadata.RefreshExternalUpload(ctx, b.namespace.ID, uploadID, expiresAt)
	if err != nil {
		return ExternalUpload{}, err
	}
	if err := b.validateExternalUpload(upload); err != nil {
		return ExternalUpload{}, err
	}
	return upload, nil
}

// CommitDirectUpload validates only immutable object geometry and the DOFS
// header before publication. AES-GCM authentication is still enforced on
// every plaintext read; streaming the entire object here would negate the
// bandwidth benefit of direct-to-object-storage uploads.
func (b *Backend) CommitDirectUpload(ctx context.Context, uploadID string) (Node, error) {
	if err := b.requireMutation(); err != nil {
		return Node{}, err
	}
	upload, err := b.GetDirectUpload(ctx, uploadID)
	if err != nil {
		return Node{}, err
	}
	if err := b.verifyDirectUploadObject(ctx, upload); err != nil {
		return Node{}, err
	}
	node, _, err := b.metadata.CommitExternalUpload(ctx, b.namespace.ID, upload.ID)
	if err != nil {
		return Node{}, err
	}
	if !b.validNode(node, false) || node.Inode != upload.Inode ||
		node.Generation != upload.TargetGeneration || node.ObjectKey != upload.ObjectKey || node.Size != upload.Size {
		return Node{}, ErrConflict
	}
	return node, nil
}

// AbortDirectUpload first detaches the durable reservation and only then
// deletes its unpublished object. This ordering cannot turn an upload/abort
// race into metadata that references deleted ciphertext.
func (b *Backend) AbortDirectUpload(ctx context.Context, uploadID string) error {
	if err := b.requireMutation(); err != nil {
		return err
	}
	upload, err := b.metadata.AbortExternalUpload(ctx, b.namespace.ID, uploadID)
	if err != nil {
		return err
	}
	if err := b.objects.Delete(ctx, upload.ObjectKey); err != nil && !errors.Is(err, ErrObjectNotFound) {
		return fmt.Errorf("delete aborted direct upload object: %w", err)
	}
	return nil
}

// AcknowledgeDirectUpload removes the committed reservation after the caller
// has durably updated its own projection/task state. Until acknowledgement,
// CommitDirectUpload remains idempotently retryable after caller crashes.
func (b *Backend) AcknowledgeDirectUpload(ctx context.Context, uploadID string) error {
	if err := b.requireMutation(); err != nil {
		return err
	}
	return b.metadata.AcknowledgeExternalUpload(ctx, b.namespace.ID, uploadID)
}

func (b *Backend) verifyDirectUploadObject(ctx context.Context, upload ExternalUpload) error {
	expected, err := CheckedEncryptedSize(upload.Size, DefaultChunkSize)
	if err != nil {
		return err
	}
	info, err := b.objects.Stat(ctx, upload.ObjectKey)
	if err != nil {
		return fmt.Errorf("stat direct upload object: %w", err)
	}
	if info.Size != expected {
		return fmt.Errorf("dofs: encrypted object size is %d, want %d", info.Size, expected)
	}
	header, err := b.objects.GetRange(ctx, upload.ObjectKey, 0, EncryptedHeaderSize-1)
	if err != nil {
		return fmt.Errorf("read direct upload header: %w", err)
	}
	chunkSize, readErr := ReadHeader(header)
	closeErr := header.Close()
	if readErr != nil {
		return fmt.Errorf("validate direct upload header: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close direct upload header: %w", closeErr)
	}
	if chunkSize != DefaultChunkSize {
		return fmt.Errorf("dofs: direct upload chunk size is %d, want %d", chunkSize, DefaultChunkSize)
	}
	return nil
}

func (b *Backend) validateExternalUpload(upload ExternalUpload) error {
	if upload.NamespaceID != b.namespace.ID || upload.Inode == 0 || upload.Size < 0 ||
		upload.TargetGeneration != upload.ExpectedGeneration+1 || len(upload.WrappedDEK) == 0 {
		return errors.New("dofs: invalid external upload metadata")
	}
	if err := ValidateExternalUploadID(upload.ID); err != nil {
		return err
	}
	if err := ValidateExternalUploadState(upload.State); err != nil {
		return err
	}
	if err := ValidateGenerationObjectKey(upload.NamespaceID, upload.Inode, upload.ObjectKey); err != nil {
		return err
	}
	return nil
}

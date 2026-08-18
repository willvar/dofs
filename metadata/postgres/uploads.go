package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/willvar/dofs"
)

const externalUploadColumns = `
namespace_id, upload_id, inode, expected_generation, target_generation,
object_key, size, wrapped_dek, creates_node, state, expires_at, created_at, committed_at`

func scanExternalUpload(row scanner) (dofs.ExternalUpload, error) {
	var upload dofs.ExternalUpload
	var committedAt sql.NullTime
	if err := row.Scan(
		&upload.NamespaceID, &upload.ID, &upload.Inode, &upload.ExpectedGeneration,
		&upload.TargetGeneration, &upload.ObjectKey, &upload.Size, &upload.WrappedDEK,
		&upload.CreatesNode, &upload.State, &upload.ExpiresAt, &upload.CreatedAt, &committedAt,
	); err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	if committedAt.Valid {
		value := committedAt.Time
		upload.CommittedAt = &value
	}
	return upload, nil
}

func validatePrepareExternalUpload(request dofs.PrepareExternalUploadRequest) error {
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return err
	}
	if err := dofs.ValidateExternalUploadID(request.ID); err != nil {
		return err
	}
	if err := dofs.ValidateName(request.Name); err != nil {
		return err
	}
	if request.Parent == 0 || request.Parent > math.MaxInt64 || request.Size < 0 ||
		len(request.WrappedDEK) == 0 || request.ExpiresAt.IsZero() {
		return errors.New("dofs: invalid external upload reservation")
	}
	return nil
}

func (s *Store) PrepareExternalUpload(ctx context.Context, request dofs.PrepareExternalUploadRequest) (dofs.ExternalUpload, error) {
	if err := validatePrepareExternalUpload(request); err != nil {
		return dofs.ExternalUpload{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, request.NamespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if _, err := requireDirectory(ctx, tx, request.NamespaceID, request.Parent); err != nil {
		return dofs.ExternalUpload{}, err
	}
	var duplicate bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dofs_external_uploads
        WHERE namespace_id = $1 AND upload_id = $2)`, request.NamespaceID, request.ID).Scan(&duplicate); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if duplicate {
		return dofs.ExternalUpload{}, dofs.ErrAlreadyExists
	}
	now := time.Now().UTC()
	if !request.ExpiresAt.After(now) {
		return dofs.ExternalUpload{}, errors.New("dofs: external upload expiry must be in the future")
	}

	node, lookupErr := scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+`
        FROM dofs_nodes WHERE namespace_id = $1 AND parent_inode = $2 AND name = $3
        AND state IN ('creating', 'ready') FOR UPDATE`, request.NamespaceID, request.Parent, request.Name))
	createsNode := false
	if lookupErr == nil {
		if !request.Replace {
			return dofs.ExternalUpload{}, dofs.ErrAlreadyExists
		}
		if !node.IsFile() {
			return dofs.ExternalUpload{}, dofs.ErrTypeMismatch
		}
		if node.State != dofs.NodeStateReady {
			return dofs.ExternalUpload{}, dofs.ErrConflict
		}
		var active bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dofs_external_uploads
            WHERE namespace_id = $1 AND inode = $2 AND state = 'active')`, request.NamespaceID, node.Inode).Scan(&active); err != nil {
			return dofs.ExternalUpload{}, err
		}
		if active {
			return dofs.ExternalUpload{}, dofs.ErrConflict
		}
	} else if !errors.Is(lookupErr, dofs.ErrNotFound) {
		return dofs.ExternalUpload{}, lookupErr
	} else {
		var inode uint64
		if err := tx.QueryRowContext(ctx, `UPDATE dofs_namespaces
            SET next_inode = next_inode + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $1 RETURNING next_inode - 1`, request.NamespaceID).Scan(&inode); err != nil {
			return dofs.ExternalUpload{}, fmt.Errorf("allocate DOFS inode: %w", err)
		}
		mode := request.Mode & 0777
		if mode == 0 {
			mode = 0600
		}
		node, err = scanNode(tx.QueryRowContext(ctx, `INSERT INTO dofs_nodes(
            namespace_id, inode, parent_inode, name, kind, state, mode, wrapped_dek
        ) VALUES ($1, $2, $3, $4, 'file', 'creating', $5, $6)
        RETURNING `+nodeColumns, request.NamespaceID, inode, request.Parent, request.Name,
			mode, request.WrappedDEK))
		if err != nil {
			return dofs.ExternalUpload{}, mapError(err)
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
	upload, err := scanExternalUpload(tx.QueryRowContext(ctx, `INSERT INTO dofs_external_uploads(
        namespace_id, upload_id, inode, expected_generation, target_generation,
        object_key, size, wrapped_dek, creates_node, state, expires_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'active', $10)
    RETURNING `+externalUploadColumns,
		request.NamespaceID, request.ID, node.Inode, node.Generation, targetGeneration,
		objectKey, request.Size, request.WrappedDEK, createsNode, request.ExpiresAt.UTC()))
	if err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	if err := tx.Commit(); err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	return upload, nil
}

func (s *Store) GetExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.ExternalUpload, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	return scanExternalUpload(s.db.QueryRowContext(ctx, `SELECT `+externalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = $1 AND upload_id = $2`, namespaceID, uploadID))
}

func (s *Store) ListExternalUploads(ctx context.Context, namespaceID string, state dofs.ExternalUploadState) ([]dofs.ExternalUpload, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if err := dofs.ValidateExternalUploadState(state); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+externalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = $1 AND state = $2
        ORDER BY created_at, upload_id`, namespaceID, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]dofs.ExternalUpload, 0)
	for rows.Next() {
		upload, err := scanExternalUpload(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, upload)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list DOFS external uploads: %w", err)
	}
	return result, nil
}

func (s *Store) RefreshExternalUpload(ctx context.Context, namespaceID, uploadID string, expiresAt time.Time) (dofs.ExternalUpload, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if !expiresAt.After(time.Now().UTC()) {
		return dofs.ExternalUpload{}, dofs.ErrConflict
	}
	upload, err := scanExternalUpload(s.db.QueryRowContext(ctx, `UPDATE dofs_external_uploads
        SET expires_at = $3 WHERE namespace_id = $1 AND upload_id = $2
        AND state = 'active' AND expires_at > CURRENT_TIMESTAMP
        RETURNING `+externalUploadColumns, namespaceID, uploadID, expiresAt.UTC()))
	return upload, mapError(err)
}

func (s *Store) CommitExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.Node, bool, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.Node{}, false, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Node{}, false, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, namespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	upload, err := scanExternalUpload(tx.QueryRowContext(ctx, `SELECT `+externalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = $1 AND upload_id = $2 FOR UPDATE`, namespaceID, uploadID))
	if err != nil {
		return dofs.Node{}, false, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+`
        FROM dofs_nodes WHERE namespace_id = $1 AND inode = $2 FOR UPDATE`, namespaceID, upload.Inode))
	if err != nil {
		return dofs.Node{}, false, err
	}
	if upload.State == dofs.ExternalUploadCommitted {
		if node.Generation == upload.TargetGeneration && node.ObjectKey == upload.ObjectKey &&
			node.Size == upload.Size && string(node.WrappedDEK) == string(upload.WrappedDEK) {
			return node, false, nil
		}
		return node, false, dofs.ErrConflict
	}
	if upload.State != dofs.ExternalUploadActive || !upload.ExpiresAt.After(time.Now().UTC()) || node.Generation != upload.ExpectedGeneration {
		return node, false, dofs.ErrConflict
	}
	if (upload.CreatesNode && node.State != dofs.NodeStateCreating) ||
		(!upload.CreatesNode && node.State != dofs.NodeStateReady) || !node.IsFile() {
		return node, false, dofs.ErrConflict
	}
	var quota, used int64
	if err := tx.QueryRowContext(ctx, `SELECT quota_bytes, used_bytes FROM dofs_namespaces WHERE id = $1`, namespaceID).Scan(&quota, &used); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	delta := upload.Size - node.Size
	if delta > 0 && (used > math.MaxInt64-delta || (quota > 0 && delta > quota-used)) {
		return node, false, dofs.ErrQuotaExceeded
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_generations(
        namespace_id, inode, number, object_key, size
    ) VALUES ($1, $2, $3, $4, $5)`, namespaceID, node.Inode,
		upload.TargetGeneration, upload.ObjectKey, upload.Size); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	nextState := node.State
	if upload.CreatesNode {
		nextState = dofs.NodeStateReady
	}
	node, err = scanNode(tx.QueryRowContext(ctx, `UPDATE dofs_nodes SET
        generation = $3, object_key = $4, size = $5, wrapped_dek = $6, state = $7,
        modified_at = CURRENT_TIMESTAMP, changed_at = CURRENT_TIMESTAMP
        WHERE namespace_id = $1 AND inode = $2 RETURNING `+nodeColumns,
		namespaceID, node.Inode, upload.TargetGeneration, upload.ObjectKey,
		upload.Size, upload.WrappedDEK, nextState))
	if err != nil {
		return dofs.Node{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_namespaces
        SET used_bytes = used_bytes + $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1`, namespaceID, delta); err != nil {
		return dofs.Node{}, false, err
	}
	var committedAt sql.NullTime
	if err := tx.QueryRowContext(ctx, `UPDATE dofs_external_uploads
        SET state = 'committed', committed_at = CURRENT_TIMESTAMP
        WHERE namespace_id = $1 AND upload_id = $2 AND state = 'active'
        RETURNING committed_at`, namespaceID, uploadID).Scan(&committedAt); err != nil {
		return dofs.Node{}, false, err
	}
	kind := dofs.EventWrite
	if upload.CreatesNode {
		kind = dofs.EventCreate
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: namespaceID, Kind: kind, Inode: node.Inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name,
	}); err != nil {
		return dofs.Node{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	return node, true, nil
}

func (s *Store) AbortExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.ExternalUpload, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	upload, err := scanExternalUpload(tx.QueryRowContext(ctx, `SELECT `+externalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = $1 AND upload_id = $2 FOR UPDATE`, namespaceID, uploadID))
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	if upload.State != dofs.ExternalUploadActive {
		return upload, dofs.ErrConflict
	}
	if upload.CreatesNode {
		result, err := tx.ExecContext(ctx, `DELETE FROM dofs_nodes WHERE namespace_id = $1 AND inode = $2
            AND state = 'creating' AND generation = 0`, namespaceID, upload.Inode)
		if err != nil {
			return dofs.ExternalUpload{}, err
		}
		affected, err := result.RowsAffected()
		if err != nil || affected != 1 {
			return upload, dofs.ErrConflict
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM dofs_external_uploads
            WHERE namespace_id = $1 AND upload_id = $2 AND state = 'active'`, namespaceID, uploadID); err != nil {
			return dofs.ExternalUpload{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	return upload, nil
}

func (s *Store) AcknowledgeExternalUpload(ctx context.Context, namespaceID, uploadID string) error {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM dofs_external_uploads
        WHERE namespace_id = $1 AND upload_id = $2 AND state = 'committed'`, namespaceID, uploadID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return dofs.ErrConflict
	}
	return nil
}

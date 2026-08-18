package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/willvar/dofs"
)

const sqliteExternalUploadColumns = `
namespace_id, upload_id, inode, expected_generation, target_generation,
object_key, size, wrapped_dek, creates_node, state, expires_at, created_at, committed_at`

func scanExternalUpload(row scanner) (dofs.ExternalUpload, error) {
	var upload dofs.ExternalUpload
	var inode, createsNode, expiresAt, createdAt int64
	var committedAt sql.NullInt64
	if err := row.Scan(
		&upload.NamespaceID, &upload.ID, &inode, &upload.ExpectedGeneration,
		&upload.TargetGeneration, &upload.ObjectKey, &upload.Size, &upload.WrappedDEK,
		&createsNode, &upload.State, &expiresAt, &createdAt, &committedAt,
	); err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	if inode <= 0 || (createsNode != 0 && createsNode != 1) {
		return dofs.ExternalUpload{}, errors.New("dofs sqlite: corrupt external upload metadata")
	}
	upload.Inode = uint64(inode)
	upload.CreatesNode = createsNode == 1
	upload.ExpiresAt = fromMillis(expiresAt)
	upload.CreatedAt = fromMillis(createdAt)
	if committedAt.Valid {
		value := fromMillis(committedAt.Int64)
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
	var duplicate int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dofs_external_uploads
        WHERE namespace_id = ? AND upload_id = ?`, request.NamespaceID, request.ID).Scan(&duplicate); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if duplicate != 0 {
		return dofs.ExternalUpload{}, dofs.ErrAlreadyExists
	}
	now := s.now().UTC()
	if !request.ExpiresAt.After(now) {
		return dofs.ExternalUpload{}, errors.New("dofs: external upload expiry must be in the future")
	}

	node, lookupErr := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND parent_inode = ? AND name = ?
        AND state IN ('creating', 'ready')`, request.NamespaceID, request.Parent, request.Name))
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
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dofs_external_uploads
            WHERE namespace_id = ? AND inode = ? AND state = 'active'`, request.NamespaceID, node.Inode).Scan(&active); err != nil {
			return dofs.ExternalUpload{}, err
		}
		if active != 0 {
			return dofs.ExternalUpload{}, dofs.ErrConflict
		}
	} else if !errors.Is(lookupErr, dofs.ErrNotFound) {
		return dofs.ExternalUpload{}, lookupErr
	} else {
		var inode int64
		if err := tx.QueryRowContext(ctx, `SELECT next_inode FROM dofs_namespaces WHERE id = ?`, request.NamespaceID).Scan(&inode); err != nil {
			return dofs.ExternalUpload{}, mapError(err)
		}
		if inode < 2 || inode == math.MaxInt64 {
			return dofs.ExternalUpload{}, errors.New("dofs sqlite: inode space exhausted")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_namespaces SET next_inode = ?, updated_at = ? WHERE id = ?`,
			inode+1, millis(now), request.NamespaceID); err != nil {
			return dofs.ExternalUpload{}, fmt.Errorf("allocate DOFS SQLite inode: %w", err)
		}
		mode := request.Mode & 0777
		if mode == 0 {
			mode = 0600
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_nodes(
            namespace_id, inode, parent_inode, name, kind, state, mode, size,
            generation, object_key, wrapped_dek, created_at, modified_at, changed_at
        ) VALUES (?, ?, ?, ?, 'file', 'creating', ?, 0, 0, '', ?, ?, ?, ?)`,
			request.NamespaceID, inode, request.Parent, request.Name, mode,
			request.WrappedDEK, millis(now), millis(now), millis(now)); err != nil {
			return dofs.ExternalUpload{}, mapError(err)
		}
		node = dofs.Node{
			NamespaceID: request.NamespaceID, Inode: uint64(inode), Parent: request.Parent,
			Name: request.Name, Kind: dofs.NodeKindFile, State: dofs.NodeStateCreating,
			Mode: mode, WrappedDEK: append([]byte(nil), request.WrappedDEK...),
			CreatedAt: now, ModifiedAt: now, ChangedAt: now,
		}
		createsNode = true
	}
	if node.Generation == math.MaxInt64 {
		return dofs.ExternalUpload{}, errors.New("dofs sqlite: generation space exhausted")
	}
	targetGeneration := node.Generation + 1
	objectKey, err := dofs.GenerationObjectKey(request.NamespaceID, node.Inode, targetGeneration, request.ID)
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_external_uploads(
        namespace_id, upload_id, inode, expected_generation, target_generation,
        object_key, size, wrapped_dek, creates_node, state, expires_at, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)`,
		request.NamespaceID, request.ID, node.Inode, node.Generation, targetGeneration,
		objectKey, request.Size, request.WrappedDEK, createsNode,
		millis(request.ExpiresAt), millis(now)); err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	if err := tx.Commit(); err != nil {
		return dofs.ExternalUpload{}, mapError(err)
	}
	return dofs.ExternalUpload{
		ID: request.ID, NamespaceID: request.NamespaceID, Inode: node.Inode,
		ExpectedGeneration: node.Generation, TargetGeneration: targetGeneration,
		ObjectKey: objectKey, Size: request.Size,
		WrappedDEK: append([]byte(nil), request.WrappedDEK...), CreatesNode: createsNode,
		State: dofs.ExternalUploadActive, ExpiresAt: request.ExpiresAt.UTC(), CreatedAt: now,
	}, nil
}

func (s *Store) GetExternalUpload(ctx context.Context, namespaceID, uploadID string) (dofs.ExternalUpload, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	if err := dofs.ValidateExternalUploadID(uploadID); err != nil {
		return dofs.ExternalUpload{}, err
	}
	return scanExternalUpload(s.db.QueryRowContext(ctx, `SELECT `+sqliteExternalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = ? AND upload_id = ?`, namespaceID, uploadID))
}

func (s *Store) ListExternalUploads(ctx context.Context, namespaceID string, state dofs.ExternalUploadState) ([]dofs.ExternalUpload, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if err := dofs.ValidateExternalUploadState(state); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sqliteExternalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = ? AND state = ?
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
		return nil, fmt.Errorf("list DOFS SQLite external uploads: %w", err)
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
	now := s.now().UTC()
	if !expiresAt.After(now) {
		return dofs.ExternalUpload{}, dofs.ErrConflict
	}
	result, err := s.db.ExecContext(ctx, `UPDATE dofs_external_uploads SET expires_at = ?
        WHERE namespace_id = ? AND upload_id = ? AND state = 'active' AND expires_at > ?`,
		millis(expiresAt.UTC()), namespaceID, uploadID, millis(now))
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	if affected != 1 {
		return dofs.ExternalUpload{}, dofs.ErrConflict
	}
	return s.GetExternalUpload(ctx, namespaceID, uploadID)
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
	upload, err := scanExternalUpload(tx.QueryRowContext(ctx, `SELECT `+sqliteExternalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = ? AND upload_id = ?`, namespaceID, uploadID))
	if err != nil {
		return dofs.Node{}, false, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, namespaceID, upload.Inode))
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
	if upload.State != dofs.ExternalUploadActive || !upload.ExpiresAt.After(s.now().UTC()) || node.Generation != upload.ExpectedGeneration {
		return node, false, dofs.ErrConflict
	}
	if (upload.CreatesNode && node.State != dofs.NodeStateCreating) ||
		(!upload.CreatesNode && node.State != dofs.NodeStateReady) || !node.IsFile() {
		return node, false, dofs.ErrConflict
	}
	var quota, used int64
	if err := tx.QueryRowContext(ctx, `SELECT quota_bytes, used_bytes FROM dofs_namespaces WHERE id = ?`, namespaceID).Scan(&quota, &used); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	delta := upload.Size - node.Size
	if delta > 0 && (used > math.MaxInt64-delta || (quota > 0 && delta > quota-used)) {
		return node, false, dofs.ErrQuotaExceeded
	}
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_generations(
        namespace_id, inode, number, object_key, size, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)`, namespaceID, node.Inode, upload.TargetGeneration,
		upload.ObjectKey, upload.Size, millis(now)); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	nextState := node.State
	if upload.CreatesNode {
		nextState = dofs.NodeStateReady
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_nodes SET generation = ?, object_key = ?,
        size = ?, wrapped_dek = ?, state = ?, modified_at = ?, changed_at = ?
        WHERE namespace_id = ? AND inode = ?`, upload.TargetGeneration, upload.ObjectKey,
		upload.Size, upload.WrappedDEK, nextState, millis(now), millis(now), namespaceID, node.Inode); err != nil {
		return dofs.Node{}, false, fmt.Errorf("publish DOFS SQLite external upload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_namespaces SET used_bytes = ?, updated_at = ? WHERE id = ?`,
		used+delta, millis(now), namespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_external_uploads SET state = 'committed', committed_at = ?
        WHERE namespace_id = ? AND upload_id = ? AND state = 'active'`, millis(now), namespaceID, uploadID); err != nil {
		return dofs.Node{}, false, err
	}
	kind := dofs.EventWrite
	if upload.CreatesNode {
		kind = dofs.EventCreate
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: namespaceID, Kind: kind, Inode: node.Inode, Generation: upload.TargetGeneration,
		Parent: node.Parent, Name: node.Name, CreatedAt: now,
	}); err != nil {
		return dofs.Node{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	node.Generation = upload.TargetGeneration
	node.ObjectKey = upload.ObjectKey
	node.Size = upload.Size
	node.WrappedDEK = append([]byte(nil), upload.WrappedDEK...)
	node.State = nextState
	node.ModifiedAt = now
	node.ChangedAt = now
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
	upload, err := scanExternalUpload(tx.QueryRowContext(ctx, `SELECT `+sqliteExternalUploadColumns+`
        FROM dofs_external_uploads WHERE namespace_id = ? AND upload_id = ?`, namespaceID, uploadID))
	if err != nil {
		return dofs.ExternalUpload{}, err
	}
	if upload.State != dofs.ExternalUploadActive {
		return upload, dofs.ErrConflict
	}
	if upload.CreatesNode {
		result, err := tx.ExecContext(ctx, `DELETE FROM dofs_nodes WHERE namespace_id = ? AND inode = ?
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
            WHERE namespace_id = ? AND upload_id = ? AND state = 'active'`, namespaceID, uploadID); err != nil {
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
        WHERE namespace_id = ? AND upload_id = ? AND state = 'committed'`, namespaceID, uploadID)
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

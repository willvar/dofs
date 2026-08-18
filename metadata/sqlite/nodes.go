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

func (s *Store) CreateNamespace(ctx context.Context, namespace dofs.Namespace) (dofs.Namespace, error) {
	if err := dofs.ValidateNamespaceID(namespace.ID); err != nil {
		return dofs.Namespace{}, err
	}
	if len(namespace.WrappedKEK) == 0 || namespace.QuotaBytes < 0 {
		return dofs.Namespace{}, dofs.ErrInvalidNamespace
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Namespace{}, err
	}
	defer tx.Rollback()
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO dofs_namespaces(id, label, wrapped_kek, quota_bytes, used_bytes, next_inode, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, 2, ?, ?)`,
		namespace.ID, namespace.Label, namespace.WrappedKEK, namespace.QuotaBytes, millis(now), millis(now),
	); err != nil {
		return dofs.Namespace{}, mapError(err)
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO dofs_nodes(
            namespace_id, inode, parent_inode, name, kind, state, mode,
            size, generation, object_key, wrapped_dek, created_at, modified_at, changed_at
        ) VALUES (?, 1, 0, '', 'directory', 'ready', ?, 0, 0, '', X'', ?, ?, ?)`,
		namespace.ID, 0700, millis(now), millis(now), millis(now),
	); err != nil {
		return dofs.Namespace{}, fmt.Errorf("create namespace root inode: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return dofs.Namespace{}, mapError(err)
	}
	return dofs.Namespace{
		ID: namespace.ID, Label: namespace.Label, WrappedKEK: append([]byte(nil), namespace.WrappedKEK...),
		QuotaBytes: namespace.QuotaBytes, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (s *Store) GetNamespace(ctx context.Context, id string) (dofs.Namespace, error) {
	if err := dofs.ValidateNamespaceID(id); err != nil {
		return dofs.Namespace{}, err
	}
	return scanNamespace(s.db.QueryRowContext(ctx, `
        SELECT id, label, wrapped_kek, quota_bytes, used_bytes, created_at, updated_at
        FROM dofs_namespaces WHERE id = ?`, id))
}

func (s *Store) ListNamespaces(ctx context.Context) ([]dofs.Namespace, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, label, wrapped_kek, quota_bytes,
        used_bytes, created_at, updated_at FROM dofs_namespaces ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list DOFS SQLite namespaces: %w", err)
	}
	defer rows.Close()
	var result []dofs.Namespace
	for rows.Next() {
		namespace, err := scanNamespace(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, namespace)
	}
	return result, rows.Err()
}

func (s *Store) GetNode(ctx context.Context, namespaceID string, inode uint64) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, err
	}
	if inode == 0 || inode > math.MaxInt64 {
		return dofs.Node{}, dofs.ErrNotFound
	}
	return scanNode(s.db.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, namespaceID, inode))
}

func (s *Store) Lookup(ctx context.Context, namespaceID string, parent uint64, name string) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, err
	}
	if err := dofs.ValidateName(name); err != nil {
		return dofs.Node{}, err
	}
	return scanNode(s.db.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes
        WHERE namespace_id = ? AND parent_inode = ? AND name = ? AND state = 'ready'`,
		namespaceID, parent, name,
	))
}

func (s *Store) List(ctx context.Context, namespaceID string, parent uint64) ([]dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	parentNode, err := s.GetNode(ctx, namespaceID, parent)
	if err != nil {
		return nil, err
	}
	if parentNode.State != dofs.NodeStateReady {
		return nil, dofs.ErrNotFound
	}
	if !parentNode.IsDir() {
		return nil, dofs.ErrNotDirectory
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes
        WHERE namespace_id = ? AND parent_inode = ? AND state = 'ready'
        ORDER BY CASE WHEN kind = 'directory' THEN 0 ELSE 1 END, name`, namespaceID, parent)
	if err != nil {
		return nil, fmt.Errorf("list DOFS SQLite directory: %w", err)
	}
	defer rows.Close()
	var result []dofs.Node
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list DOFS SQLite directory: %w", err)
	}
	return result, nil
}

func (s *Store) CreateNode(ctx context.Context, request dofs.CreateNodeRequest) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
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
	if request.Kind == dofs.NodeKindDirectory && len(request.WrappedDEK) != 0 {
		return dofs.Node{}, errors.New("dofs: directory cannot have a wrapped data key")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Node{}, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, request.NamespaceID); err != nil {
		return dofs.Node{}, err
	}
	if _, err := requireDirectory(ctx, tx, request.NamespaceID, request.Parent); err != nil {
		return dofs.Node{}, err
	}
	if exists, err := activeNameExists(ctx, tx, request.NamespaceID, request.Parent, request.Name); err != nil {
		return dofs.Node{}, err
	} else if exists {
		return dofs.Node{}, dofs.ErrAlreadyExists
	}
	var inode int64
	if err := tx.QueryRowContext(ctx, `SELECT next_inode FROM dofs_namespaces WHERE id = ?`, request.NamespaceID).Scan(&inode); err != nil {
		return dofs.Node{}, mapError(err)
	}
	if inode < 2 || inode == math.MaxInt64 {
		return dofs.Node{}, errors.New("dofs sqlite: inode space exhausted")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_namespaces SET next_inode = ?, updated_at = ? WHERE id = ?`,
		inode+1, millis(s.now()), request.NamespaceID,
	); err != nil {
		return dofs.Node{}, fmt.Errorf("allocate DOFS SQLite inode: %w", err)
	}
	mode := request.Mode & 0777
	if mode == 0 && request.Kind == dofs.NodeKindFile {
		mode = 0600
	}
	if mode == 0 && request.Kind == dofs.NodeKindDirectory {
		mode = 0700
	}
	state := dofs.NodeStateCreating
	if request.Kind == dofs.NodeKindDirectory {
		state = dofs.NodeStateReady
	}
	now := s.now().UTC()
	wrappedDEK := request.WrappedDEK
	if wrappedDEK == nil {
		wrappedDEK = []byte{}
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO dofs_nodes(
            namespace_id, inode, parent_inode, name, kind, state, mode, size,
            generation, object_key, wrapped_dek, created_at, modified_at, changed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, '', ?, ?, ?, ?)`,
		request.NamespaceID, inode, request.Parent, request.Name, request.Kind, state, mode,
		wrappedDEK, millis(now), millis(now), millis(now),
	); err != nil {
		return dofs.Node{}, mapError(err)
	}
	node := dofs.Node{
		NamespaceID: request.NamespaceID, Inode: uint64(inode), Parent: request.Parent,
		Name: request.Name, Kind: request.Kind, State: state, Mode: mode,
		WrappedDEK: append([]byte(nil), wrappedDEK...), CreatedAt: now, ModifiedAt: now, ChangedAt: now,
	}
	if node.IsDir() {
		if err := insertEvent(ctx, tx, dofs.Event{
			NamespaceID: node.NamespaceID, Kind: dofs.EventCreate, Inode: node.Inode,
			Parent: node.Parent, Name: node.Name, CreatedAt: now,
		}); err != nil {
			return dofs.Node{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, mapError(err)
	}
	return node, nil
}

func (s *Store) AbortCreate(ctx context.Context, namespaceID string, inode uint64) error {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return err
	}
	if inode == 0 || inode > math.MaxInt64 {
		return dofs.ErrNotFound
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, namespaceID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dofs_nodes
        WHERE namespace_id = ? AND inode = ? AND state = 'creating' AND generation = 0`, namespaceID, inode)
	if err != nil {
		return fmt.Errorf("abort DOFS SQLite file creation: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
            SELECT 1 FROM dofs_nodes WHERE namespace_id = ? AND inode = ?)`, namespaceID, inode).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return dofs.ErrConflict
		}
	}
	return mapError(tx.Commit())
}

func lockNamespace(ctx context.Context, tx *sql.Tx, namespaceID string) error {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM dofs_namespaces WHERE id = ?`, namespaceID).Scan(&id); err != nil {
		return mapError(err)
	}
	return nil
}

func requireDirectory(ctx context.Context, tx *sql.Tx, namespaceID string, inode uint64) (dofs.Node, error) {
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, namespaceID, inode))
	if err != nil {
		return dofs.Node{}, err
	}
	if node.State != dofs.NodeStateReady {
		return dofs.Node{}, dofs.ErrNotFound
	}
	if !node.IsDir() {
		return dofs.Node{}, dofs.ErrNotDirectory
	}
	return node, nil
}

func activeNameExists(ctx context.Context, tx *sql.Tx, namespaceID string, parent uint64, name string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
        SELECT 1 FROM dofs_nodes
        WHERE namespace_id = ? AND parent_inode = ? AND name = ? AND state IN ('creating', 'ready'))`,
		namespaceID, parent, name,
	).Scan(&exists)
	return exists, err
}

type scanner interface {
	Scan(...any) error
}

func scanNamespace(row scanner) (dofs.Namespace, error) {
	var namespace dofs.Namespace
	var createdAt, updatedAt int64
	if err := row.Scan(
		&namespace.ID, &namespace.Label, &namespace.WrappedKEK, &namespace.QuotaBytes,
		&namespace.UsedBytes, &createdAt, &updatedAt,
	); err != nil {
		return dofs.Namespace{}, mapError(err)
	}
	namespace.CreatedAt = fromMillis(createdAt)
	namespace.UpdatedAt = fromMillis(updatedAt)
	return namespace, nil
}

func scanNode(row scanner) (dofs.Node, error) {
	var node dofs.Node
	var inode, parent, mode int64
	var createdAt, modifiedAt, changedAt int64
	var deletedAt sql.NullInt64
	if err := row.Scan(
		&node.NamespaceID, &inode, &parent, &node.Name, &node.Kind, &node.State,
		&mode, &node.Size, &node.Generation, &node.ObjectKey, &node.WrappedDEK,
		&createdAt, &modifiedAt, &changedAt, &deletedAt,
	); err != nil {
		return dofs.Node{}, mapError(err)
	}
	if inode <= 0 || parent < 0 || mode < 0 || mode > 0777 {
		return dofs.Node{}, errors.New("dofs sqlite: corrupt node metadata")
	}
	node.Inode = uint64(inode)
	node.Parent = uint64(parent)
	node.Mode = uint32(mode)
	node.CreatedAt = fromMillis(createdAt)
	node.ModifiedAt = fromMillis(modifiedAt)
	node.ChangedAt = fromMillis(changedAt)
	if deletedAt.Valid {
		value := fromMillis(deletedAt.Int64)
		node.DeletedAt = &value
	}
	return node, nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, event dofs.Event) error {
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO dofs_events(
        namespace_id, kind, inode, generation, parent_inode, name,
        old_parent_inode, old_name, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.NamespaceID, event.Kind, event.Inode, event.Generation, event.Parent,
		event.Name, event.OldParent, event.OldName, millis(createdAt),
	)
	if err != nil {
		return fmt.Errorf("append DOFS SQLite metadata event: %w", err)
	}
	return nil
}

var _ dofs.MetadataStore = (*Store)(nil)

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/willvar/dofs"
)

const nodeColumns = `
namespace_id, inode, parent_inode, name, kind, state, mode, size, generation,
object_key, wrapped_dek, created_at, modified_at, changed_at, deleted_at`

type Store struct {
	db *sql.DB
}

func New(database *sql.DB) (*Store, error) {
	if database == nil {
		return nil, errors.New("dofs postgres: database is required")
	}
	return &Store{db: database}, nil
}

// Migrate installs the versioned metadata schema. It takes a transaction-level
// advisory lock so multiple DOFS processes may safely race during deployment.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin DOFS schema migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('dofs:schema', 0))`); err != nil {
		return fmt.Errorf("lock DOFS schema migration: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS dofs_schema (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            version INTEGER NOT NULL CHECK (version >= 0)
        )`); err != nil {
		return fmt.Errorf("create DOFS schema version table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_schema(singleton, version) VALUES (TRUE, 0) ON CONFLICT (singleton) DO NOTHING`); err != nil {
		return fmt.Errorf("initialize DOFS schema version: %w", err)
	}
	var current int
	if err := tx.QueryRowContext(ctx, `SELECT version FROM dofs_schema WHERE singleton = TRUE FOR UPDATE`).Scan(&current); err != nil {
		return fmt.Errorf("read DOFS schema version: %w", err)
	}
	if current > schemaVersion {
		return fmt.Errorf("DOFS metadata schema %d is newer than supported version %d", current, schemaVersion)
	}
	if current < 1 {
		if _, err := tx.ExecContext(ctx, createSchema); err != nil {
			return fmt.Errorf("install DOFS metadata schema v1: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 1 WHERE singleton = TRUE`); err != nil {
			return fmt.Errorf("record DOFS metadata schema v1: %w", err)
		}
		current = 1
	}
	if current < 2 {
		if _, err := tx.ExecContext(ctx, createExternalUploadSchema); err != nil {
			return fmt.Errorf("install DOFS metadata schema v2: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 2 WHERE singleton = TRUE`); err != nil {
			return fmt.Errorf("record DOFS metadata schema v2: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DOFS schema migration: %w", err)
	}
	return nil
}

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
	row := tx.QueryRowContext(ctx, `
        INSERT INTO dofs_namespaces(id, label, wrapped_kek, quota_bytes)
        VALUES ($1, $2, $3, $4)
        RETURNING id, label, wrapped_kek, quota_bytes, used_bytes, created_at, updated_at`,
		namespace.ID, namespace.Label, namespace.WrappedKEK, namespace.QuotaBytes,
	)
	created, err := scanNamespace(row)
	if err != nil {
		return dofs.Namespace{}, mapError(err)
	}
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO dofs_nodes(
            namespace_id, inode, parent_inode, name, kind, state, mode,
            created_at, modified_at, changed_at
        ) VALUES ($1, 1, 0, '', 'directory', 'ready', 448, $2, $2, $2)`,
		created.ID, created.CreatedAt,
	); err != nil {
		return dofs.Namespace{}, fmt.Errorf("create namespace root inode: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return dofs.Namespace{}, mapError(err)
	}
	return created, nil
}

func (s *Store) GetNamespace(ctx context.Context, id string) (dofs.Namespace, error) {
	if err := dofs.ValidateNamespaceID(id); err != nil {
		return dofs.Namespace{}, err
	}
	return scanNamespace(s.db.QueryRowContext(ctx, `
        SELECT id, label, wrapped_kek, quota_bytes, used_bytes, created_at, updated_at
        FROM dofs_namespaces WHERE id = $1`, id))
}

func (s *Store) ListNamespaces(ctx context.Context) ([]dofs.Namespace, error) {
	rows, err := s.db.QueryContext(ctx, `
        SELECT id, label, wrapped_kek, quota_bytes, used_bytes, created_at, updated_at
        FROM dofs_namespaces ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list DOFS namespaces: %w", err)
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

func (s *Store) DeleteNamespace(ctx context.Context, id string) error {
	if err := dofs.ValidateNamespaceID(id); err != nil {
		return err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	key := "dofs:v1:writer:" + id
	var acquired bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`, key).Scan(&acquired); err != nil {
		return fmt.Errorf("lock DOFS namespace deletion: %w", err)
	}
	if !acquired {
		return dofs.ErrWriterBusy
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dofs_namespaces WHERE id = $1`, id)
	if err != nil {
		return mapError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return dofs.ErrNotFound
	}
	if err := tx.Commit(); err != nil {
		return mapError(err)
	}
	return nil
}

func (s *Store) GetNode(ctx context.Context, namespaceID string, inode uint64) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, err
	}
	return scanNode(s.db.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM dofs_nodes WHERE namespace_id = $1 AND inode = $2`, namespaceID, inode))
}

func (s *Store) Lookup(ctx context.Context, namespaceID string, parent uint64, name string) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, err
	}
	if err := dofs.ValidateName(name); err != nil {
		return dofs.Node{}, err
	}
	return scanNode(s.db.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND parent_inode = $2 AND name = $3 AND state = 'ready'`,
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
	rows, err := s.db.QueryContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND parent_inode = $2 AND state = 'ready'
        ORDER BY CASE WHEN kind = 'directory' THEN 0 ELSE 1 END, name`, namespaceID, parent)
	if err != nil {
		return nil, fmt.Errorf("list DOFS directory: %w", err)
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
		return nil, fmt.Errorf("list DOFS directory: %w", err)
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
	var inode uint64
	if err := tx.QueryRowContext(ctx, `
        UPDATE dofs_namespaces SET next_inode = next_inode + 1, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1 RETURNING next_inode - 1`, request.NamespaceID).Scan(&inode); err != nil {
		return dofs.Node{}, fmt.Errorf("allocate DOFS inode: %w", err)
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
	wrappedDEK := request.WrappedDEK
	if wrappedDEK == nil {
		wrappedDEK = []byte{}
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `
        INSERT INTO dofs_nodes(
            namespace_id, inode, parent_inode, name, kind, state, mode, wrapped_dek
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING `+nodeColumns,
		request.NamespaceID, inode, request.Parent, request.Name, request.Kind, state, mode, wrappedDEK,
	))
	if err != nil {
		return dofs.Node{}, mapError(err)
	}
	if node.IsDir() {
		if err := insertEvent(ctx, tx, dofs.Event{
			NamespaceID: node.NamespaceID, Kind: dofs.EventCreate, Inode: node.Inode,
			Parent: node.Parent, Name: node.Name,
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
	if inode == 0 {
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
	result, err := tx.ExecContext(ctx, `
        DELETE FROM dofs_nodes
        WHERE namespace_id = $1 AND inode = $2 AND state = 'creating' AND generation = 0`, namespaceID, inode)
	if err != nil {
		return fmt.Errorf("abort DOFS file creation: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected == 0 {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dofs_nodes WHERE namespace_id = $1 AND inode = $2)`, namespaceID, inode).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return dofs.ErrConflict
		}
	}
	return tx.Commit()
}

func (s *Store) begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("begin DOFS metadata transaction: %w", err)
	}
	return tx, nil
}

func lockNamespace(ctx context.Context, tx *sql.Tx, namespaceID string) error {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM dofs_namespaces WHERE id = $1 FOR UPDATE`, namespaceID).Scan(&id); err != nil {
		return mapError(err)
	}
	return nil
}

func requireDirectory(ctx context.Context, tx *sql.Tx, namespaceID string, inode uint64) (dofs.Node, error) {
	node, err := scanNode(tx.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND inode = $2 FOR UPDATE`, namespaceID, inode))
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
	err := tx.QueryRowContext(ctx, `
        SELECT EXISTS(
            SELECT 1 FROM dofs_nodes
            WHERE namespace_id = $1 AND parent_inode = $2 AND name = $3 AND state IN ('creating', 'ready')
        )`, namespaceID, parent, name).Scan(&exists)
	return exists, err
}

type scanner interface {
	Scan(...any) error
}

func scanNamespace(row scanner) (dofs.Namespace, error) {
	var namespace dofs.Namespace
	err := row.Scan(
		&namespace.ID, &namespace.Label, &namespace.WrappedKEK, &namespace.QuotaBytes,
		&namespace.UsedBytes, &namespace.CreatedAt, &namespace.UpdatedAt,
	)
	if err != nil {
		return dofs.Namespace{}, mapError(err)
	}
	return namespace, nil
}

func scanNode(row scanner) (dofs.Node, error) {
	var node dofs.Node
	var deletedAt sql.NullTime
	err := row.Scan(
		&node.NamespaceID, &node.Inode, &node.Parent, &node.Name, &node.Kind, &node.State,
		&node.Mode, &node.Size, &node.Generation, &node.ObjectKey, &node.WrappedDEK,
		&node.CreatedAt, &node.ModifiedAt, &node.ChangedAt, &deletedAt,
	)
	if err != nil {
		return dofs.Node{}, mapError(err)
	}
	if deletedAt.Valid {
		node.DeletedAt = &deletedAt.Time
	}
	return node, nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, event dofs.Event) error {
	_, err := tx.ExecContext(ctx, `
        INSERT INTO dofs_events(
            namespace_id, kind, inode, generation, parent_inode, name, old_parent_inode, old_name
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		event.NamespaceID, event.Kind, event.Inode, event.Generation,
		event.Parent, event.Name, event.OldParent, event.OldName,
	)
	if err != nil {
		return fmt.Errorf("append DOFS metadata event: %w", err)
	}
	return nil
}

type sqlStateError interface {
	SQLState() string
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return dofs.ErrNotFound
	}
	var state sqlStateError
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return dofs.ErrAlreadyExists
		case "40001", "40P01":
			return fmt.Errorf("%w: retry metadata transaction", dofs.ErrConflict)
		}
	}
	return err
}

var _ dofs.MetadataStore = (*Store)(nil)

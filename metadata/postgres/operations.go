package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/willvar/dofs"
)

func (s *Store) CommitGeneration(ctx context.Context, request dofs.CommitGenerationRequest) (dofs.Node, bool, error) {
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	if request.Inode == 0 || request.Size < 0 {
		return dofs.Node{}, false, errors.New("dofs: invalid generation")
	}
	if err := dofs.ValidateGenerationObjectKey(request.NamespaceID, request.Inode, request.ObjectKey); err != nil {
		return dofs.Node{}, false, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Node{}, false, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, request.NamespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND inode = $2 FOR UPDATE`, request.NamespaceID, request.Inode))
	if err != nil {
		return dofs.Node{}, false, err
	}
	if !node.IsFile() || (node.State != dofs.NodeStateCreating && node.State != dofs.NodeStateReady && node.State != dofs.NodeStateDeleted) {
		return dofs.Node{}, false, dofs.ErrNotFound
	}
	if node.Generation != request.ExpectedGeneration {
		return node, false, nil
	}
	var quota, used int64
	if err := tx.QueryRowContext(ctx, `SELECT quota_bytes, used_bytes FROM dofs_namespaces WHERE id = $1`, request.NamespaceID).Scan(&quota, &used); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	delta := request.Size - node.Size
	if quota > 0 && delta > 0 && used+delta > quota {
		return node, false, dofs.ErrQuotaExceeded
	}
	nextGeneration := node.Generation + 1
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO dofs_generations(namespace_id, inode, number, object_key, size)
        VALUES ($1, $2, $3, $4, $5)`,
		request.NamespaceID, request.Inode, nextGeneration, request.ObjectKey, request.Size,
	); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	wasCreating := node.State == dofs.NodeStateCreating
	nextState := node.State
	if wasCreating {
		nextState = dofs.NodeStateReady
	}
	node, err = scanNode(tx.QueryRowContext(ctx, `
        UPDATE dofs_nodes
        SET generation = $3, object_key = $4, size = $5, state = $6,
            modified_at = CURRENT_TIMESTAMP, changed_at = CURRENT_TIMESTAMP
        WHERE namespace_id = $1 AND inode = $2
        RETURNING `+nodeColumns,
		request.NamespaceID, request.Inode, nextGeneration, request.ObjectKey, request.Size, nextState,
	))
	if err != nil {
		return dofs.Node{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
        UPDATE dofs_namespaces
        SET used_bytes = used_bytes + $2, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1`, request.NamespaceID, delta); err != nil {
		return dofs.Node{}, false, fmt.Errorf("update DOFS namespace usage: %w", err)
	}
	kind := dofs.EventWrite
	if wasCreating {
		kind = dofs.EventCreate
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: kind, Inode: node.Inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name,
	}); err != nil {
		return dofs.Node{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	return node, true, nil
}

func (s *Store) Rename(ctx context.Context, request dofs.RenameRequest) (dofs.Node, *dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return dofs.Node{}, nil, err
	}
	if err := dofs.ValidateName(request.NewName); err != nil {
		return dofs.Node{}, nil, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Node{}, nil, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, request.NamespaceID); err != nil {
		return dofs.Node{}, nil, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND inode = $2 FOR UPDATE`, request.NamespaceID, request.Inode))
	if err != nil {
		return dofs.Node{}, nil, err
	}
	if node.Inode == dofs.RootInode || node.State != dofs.NodeStateReady {
		return dofs.Node{}, nil, dofs.ErrNotFound
	}
	if _, err := requireDirectory(ctx, tx, request.NamespaceID, request.NewParent); err != nil {
		return dofs.Node{}, nil, err
	}
	if node.IsDir() {
		var cycle bool
		if err := tx.QueryRowContext(ctx, `
            WITH RECURSIVE ancestors(inode, parent_inode) AS (
                SELECT inode, parent_inode FROM dofs_nodes
                WHERE namespace_id = $1 AND inode = $2
                UNION ALL
                SELECT parent.inode, parent.parent_inode
                FROM dofs_nodes parent
                JOIN ancestors child ON parent.inode = child.parent_inode
                WHERE parent.namespace_id = $1 AND child.parent_inode <> 0
            )
            SELECT EXISTS(SELECT 1 FROM ancestors WHERE inode = $3)`,
			request.NamespaceID, request.NewParent, node.Inode,
		).Scan(&cycle); err != nil {
			return dofs.Node{}, nil, fmt.Errorf("check DOFS directory rename cycle: %w", err)
		}
		if cycle {
			return dofs.Node{}, nil, dofs.ErrInvalidName
		}
	}
	if node.Parent == request.NewParent && node.Name == request.NewName {
		return node, nil, nil
	}

	var replaced *dofs.Node
	destination, destinationErr := scanNode(tx.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND parent_inode = $2 AND name = $3 AND state IN ('creating', 'ready')
        FOR UPDATE`, request.NamespaceID, request.NewParent, request.NewName))
	if destinationErr == nil {
		if destination.Inode == node.Inode {
			return node, nil, nil
		}
		if request.NoReplace {
			return dofs.Node{}, nil, dofs.ErrAlreadyExists
		}
		if destination.State != dofs.NodeStateReady || destination.Kind != node.Kind {
			return dofs.Node{}, nil, dofs.ErrTypeMismatch
		}
		if destination.IsDir() {
			var nonempty bool
			if err := tx.QueryRowContext(ctx, `
                SELECT EXISTS(SELECT 1 FROM dofs_nodes
                    WHERE namespace_id = $1 AND parent_inode = $2 AND state IN ('creating', 'ready'))`,
				request.NamespaceID, destination.Inode,
			).Scan(&nonempty); err != nil {
				return dofs.Node{}, nil, err
			}
			if nonempty {
				return dofs.Node{}, nil, dofs.ErrDirectoryNotEmpty
			}
		}
		replacedNode, err := scanNode(tx.QueryRowContext(ctx, `
            UPDATE dofs_nodes
            SET parent_inode = 0, state = 'deleted', deleted_at = CURRENT_TIMESTAMP,
                changed_at = CURRENT_TIMESTAMP
            WHERE namespace_id = $1 AND inode = $2
            RETURNING `+nodeColumns, request.NamespaceID, destination.Inode))
		if err != nil {
			return dofs.Node{}, nil, err
		}
		if err := insertEvent(ctx, tx, dofs.Event{
			NamespaceID: destination.NamespaceID, Kind: dofs.EventRemove, Inode: destination.Inode,
			Generation: destination.Generation, OldParent: destination.Parent, OldName: destination.Name,
		}); err != nil {
			return dofs.Node{}, nil, err
		}
		replaced = &replacedNode
	} else if !errors.Is(destinationErr, dofs.ErrNotFound) {
		return dofs.Node{}, nil, destinationErr
	}

	oldParent, oldName := node.Parent, node.Name
	node, err = scanNode(tx.QueryRowContext(ctx, `
        UPDATE dofs_nodes
        SET parent_inode = $3, name = $4, changed_at = CURRENT_TIMESTAMP
        WHERE namespace_id = $1 AND inode = $2
        RETURNING `+nodeColumns,
		request.NamespaceID, node.Inode, request.NewParent, request.NewName,
	))
	if err != nil {
		return dofs.Node{}, nil, mapError(err)
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: dofs.EventRename, Inode: node.Inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name,
		OldParent: oldParent, OldName: oldName,
	}); err != nil {
		return dofs.Node{}, nil, err
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, nil, mapError(err)
	}
	return node, replaced, nil
}

func (s *Store) Remove(ctx context.Context, request dofs.RemoveRequest) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return dofs.Node{}, err
	}
	if err := dofs.ValidateName(request.Name); err != nil {
		return dofs.Node{}, err
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
	node, err := scanNode(tx.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND parent_inode = $2 AND name = $3 AND state = 'ready'
        FOR UPDATE`, request.NamespaceID, request.Parent, request.Name))
	if err != nil {
		return dofs.Node{}, err
	}
	if node.IsDir() != request.Directory {
		return dofs.Node{}, dofs.ErrTypeMismatch
	}
	if node.IsDir() {
		var nonempty bool
		if err := tx.QueryRowContext(ctx, `
            SELECT EXISTS(SELECT 1 FROM dofs_nodes
                WHERE namespace_id = $1 AND parent_inode = $2 AND state IN ('creating', 'ready'))`,
			request.NamespaceID, node.Inode,
		).Scan(&nonempty); err != nil {
			return dofs.Node{}, err
		}
		if nonempty {
			return dofs.Node{}, dofs.ErrDirectoryNotEmpty
		}
	}
	oldParent, oldName := node.Parent, node.Name
	node, err = scanNode(tx.QueryRowContext(ctx, `
        UPDATE dofs_nodes
        SET parent_inode = 0, state = 'deleted', deleted_at = CURRENT_TIMESTAMP,
            changed_at = CURRENT_TIMESTAMP
        WHERE namespace_id = $1 AND inode = $2
        RETURNING `+nodeColumns, request.NamespaceID, node.Inode))
	if err != nil {
		return dofs.Node{}, err
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: dofs.EventRemove, Inode: node.Inode,
		Generation: node.Generation, OldParent: oldParent, OldName: oldName,
	}); err != nil {
		return dofs.Node{}, err
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, mapError(err)
	}
	return node, nil
}

func (s *Store) Purge(ctx context.Context, namespaceID string, inode uint64) (bool, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return false, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, namespaceID); err != nil {
		return false, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND inode = $2 FOR UPDATE`, namespaceID, inode))
	if errors.Is(err, dofs.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if node.State != dofs.NodeStateDeleted {
		return false, dofs.ErrConflict
	}
	if node.IsDir() {
		var nonempty bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dofs_nodes WHERE namespace_id = $1 AND parent_inode = $2)`, namespaceID, inode).Scan(&nonempty); err != nil {
			return false, err
		}
		if nonempty {
			return false, dofs.ErrDirectoryNotEmpty
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dofs_nodes WHERE namespace_id = $1 AND inode = $2`, namespaceID, inode); err != nil {
		return false, fmt.Errorf("purge DOFS inode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
        UPDATE dofs_namespaces
        SET used_bytes = used_bytes - $2, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1`, namespaceID, node.Size); err != nil {
		return false, fmt.Errorf("release DOFS namespace usage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, mapError(err)
	}
	return true, nil
}

func (s *Store) ListGenerations(ctx context.Context, namespaceID string, inode uint64) ([]dofs.Generation, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if inode == 0 {
		return nil, dofs.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT namespace_id, inode, number, object_key, size, created_at
        FROM dofs_generations WHERE namespace_id = $1 AND inode = $2 ORDER BY number`, namespaceID, inode)
	if err != nil {
		return nil, fmt.Errorf("list DOFS generations: %w", err)
	}
	defer rows.Close()
	var result []dofs.Generation
	for rows.Next() {
		var generation dofs.Generation
		if err := rows.Scan(
			&generation.NamespaceID, &generation.Inode, &generation.Number,
			&generation.ObjectKey, &generation.Size, &generation.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan DOFS generation: %w", err)
		}
		result = append(result, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list DOFS generations: %w", err)
	}
	if len(result) == 0 {
		var exists bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dofs_nodes WHERE namespace_id = $1 AND inode = $2)`, namespaceID, inode).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, dofs.ErrNotFound
		}
	}
	return result, nil
}

func (s *Store) ListState(ctx context.Context, namespaceID string, state dofs.NodeState) ([]dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if state != dofs.NodeStateCreating && state != dofs.NodeStateReady && state != dofs.NodeStateDeleted {
		return nil, errors.New("dofs: invalid node state")
	}
	if _, err := s.GetNamespace(ctx, namespaceID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT `+nodeColumns+` FROM dofs_nodes
        WHERE namespace_id = $1 AND state = $2 ORDER BY inode`, namespaceID, state)
	if err != nil {
		return nil, fmt.Errorf("list DOFS inode state: %w", err)
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
	return result, rows.Err()
}

func (s *Store) Chmod(ctx context.Context, namespaceID string, inode uint64, mode uint32) (dofs.Node, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return dofs.Node{}, err
	}
	if inode == 0 {
		return dofs.Node{}, dofs.ErrNotFound
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Node{}, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, namespaceID); err != nil {
		return dofs.Node{}, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `
        UPDATE dofs_nodes SET mode = $3, changed_at = CURRENT_TIMESTAMP
        WHERE namespace_id = $1 AND inode = $2 AND state = 'ready'
        RETURNING `+nodeColumns, namespaceID, inode, mode&0777))
	if err != nil {
		return dofs.Node{}, err
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: namespaceID, Kind: dofs.EventChmod, Inode: inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name,
	}); err != nil {
		return dofs.Node{}, err
	}
	if err := tx.Commit(); err != nil {
		return dofs.Node{}, mapError(err)
	}
	return node, nil
}

func (s *Store) ListEvents(ctx context.Context, namespaceID string, after int64, limit int) ([]dofs.Event, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if _, err := s.GetNamespace(ctx, namespaceID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT sequence, namespace_id, kind, inode, generation, parent_inode,
               name, old_parent_inode, old_name, created_at
        FROM dofs_events
        WHERE namespace_id = $1 AND sequence > $2
        ORDER BY sequence LIMIT $3`, namespaceID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list DOFS events: %w", err)
	}
	defer rows.Close()
	var result []dofs.Event
	for rows.Next() {
		var event dofs.Event
		if err := rows.Scan(
			&event.Sequence, &event.NamespaceID, &event.Kind, &event.Inode, &event.Generation,
			&event.Parent, &event.Name, &event.OldParent, &event.OldName, &event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan DOFS event: %w", err)
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *Store) LatestEventSequence(ctx context.Context, namespaceID string) (int64, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return 0, err
	}
	var sequence int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM dofs_events WHERE namespace_id = $1`, namespaceID).
		Scan(&sequence); err != nil {
		return 0, fmt.Errorf("read latest DOFS event sequence: %w", err)
	}
	return sequence, nil
}

func (s *Store) AcquireWriterLease(ctx context.Context, namespaceID string) (dofs.Lease, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve DOFS writer connection: %w", err)
	}
	var exists bool
	if err := conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dofs_namespaces WHERE id = $1)`, namespaceID).Scan(&exists); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("verify DOFS writer namespace: %w", err)
	}
	if !exists {
		_ = conn.Close()
		return nil, dofs.ErrNotFound
	}
	key := "dofs:v1:writer:" + namespaceID
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, key).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire DOFS writer lease: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, dofs.ErrWriterBusy
	}
	lease := &writerLease{
		conn: conn, key: key, lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go lease.monitor()
	return lease, nil
}

type writerLease struct {
	mu   sync.Mutex
	conn *sql.Conn
	key  string
	lost chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once
	err  error
}

func (l *writerLease) Lost() <-chan struct{} { return l.lost }

func (l *writerLease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *writerLease) monitor() {
	defer close(l.done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			l.mu.Lock()
			conn := l.conn
			var one int
			var err error
			if conn == nil {
				err = errors.New("writer lease connection is closed")
			} else {
				err = conn.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
				if err == nil && one != 1 {
					err = errors.New("writer lease heartbeat returned an invalid value")
				}
			}
			cancel()
			if err != nil {
				l.err = fmt.Errorf("DOFS writer lease lost: %w", err)
				select {
				case <-l.lost:
				default:
					close(l.lost)
				}
				l.mu.Unlock()
				return
			}
			l.mu.Unlock()
		}
	}
}

func (l *writerLease) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		close(l.stop)
		<-l.done
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.conn == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released bool
		if err := l.conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, l.key).Scan(&released); err != nil {
			if l.err == nil {
				l.err = err
			}
		} else if !released && l.err == nil {
			l.err = errors.New("DOFS writer lease was not held")
		}
		if err := l.conn.Close(); err != nil && l.err == nil {
			l.err = err
		}
		l.conn = nil
	})
	return l.Err()
}

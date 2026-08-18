package sqlite

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/willvar/dofs"
)

func (s *Store) CommitGeneration(ctx context.Context, request dofs.CommitGenerationRequest) (dofs.Node, bool, error) {
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return dofs.Node{}, false, err
	}
	if request.Inode == 0 || request.Inode > math.MaxInt64 || request.Size < 0 {
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
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, request.NamespaceID, request.Inode))
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
	if err := tx.QueryRowContext(ctx, `SELECT quota_bytes, used_bytes FROM dofs_namespaces WHERE id = ?`, request.NamespaceID).Scan(&quota, &used); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	delta := request.Size - node.Size
	if delta > 0 && (used > math.MaxInt64-delta || (quota > 0 && delta > quota-used)) {
		return node, false, dofs.ErrQuotaExceeded
	}
	if node.Generation == math.MaxInt64 {
		return node, false, errors.New("dofs sqlite: generation space exhausted")
	}
	nextGeneration := node.Generation + 1
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_generations(
        namespace_id, inode, number, object_key, size, created_at
    ) VALUES (?, ?, ?, ?, ?, ?)`,
		request.NamespaceID, request.Inode, nextGeneration, request.ObjectKey, request.Size, millis(now),
	); err != nil {
		return dofs.Node{}, false, mapError(err)
	}
	wasCreating := node.State == dofs.NodeStateCreating
	if wasCreating {
		node.State = dofs.NodeStateReady
	}
	node.Generation = nextGeneration
	node.ObjectKey = request.ObjectKey
	node.Size = request.Size
	node.ModifiedAt = now
	node.ChangedAt = now
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_nodes
        SET generation = ?, object_key = ?, size = ?, state = ?, modified_at = ?, changed_at = ?
        WHERE namespace_id = ? AND inode = ?`,
		node.Generation, node.ObjectKey, node.Size, node.State, millis(now), millis(now),
		request.NamespaceID, request.Inode,
	); err != nil {
		return dofs.Node{}, false, fmt.Errorf("advance DOFS SQLite generation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_namespaces
        SET used_bytes = ?, updated_at = ? WHERE id = ?`, used+delta, millis(now), request.NamespaceID); err != nil {
		return dofs.Node{}, false, fmt.Errorf("update DOFS SQLite namespace usage: %w", err)
	}
	kind := dofs.EventWrite
	if wasCreating {
		kind = dofs.EventCreate
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: kind, Inode: node.Inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name, CreatedAt: now,
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
	if request.Inode == 0 || request.Inode > math.MaxInt64 {
		return dofs.Node{}, nil, dofs.ErrNotFound
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return dofs.Node{}, nil, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, request.NamespaceID); err != nil {
		return dofs.Node{}, nil, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, request.NamespaceID, request.Inode))
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
		if err := tx.QueryRowContext(ctx, `WITH RECURSIVE ancestors(inode, parent_inode) AS (
            SELECT inode, parent_inode FROM dofs_nodes
            WHERE namespace_id = ? AND inode = ?
            UNION ALL
            SELECT parent.inode, parent.parent_inode
            FROM dofs_nodes AS parent
            JOIN ancestors AS child ON parent.inode = child.parent_inode
            WHERE parent.namespace_id = ? AND child.parent_inode <> 0
        ) SELECT EXISTS(SELECT 1 FROM ancestors WHERE inode = ?)`,
			request.NamespaceID, request.NewParent, request.NamespaceID, node.Inode,
		).Scan(&cycle); err != nil {
			return dofs.Node{}, nil, fmt.Errorf("check DOFS SQLite directory cycle: %w", err)
		}
		if cycle {
			return dofs.Node{}, nil, dofs.ErrInvalidName
		}
	}
	if node.Parent == request.NewParent && node.Name == request.NewName {
		return node, nil, nil
	}

	var replaced *dofs.Node
	destination, destinationErr := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes
        WHERE namespace_id = ? AND parent_inode = ? AND name = ? AND state IN ('creating', 'ready')`,
		request.NamespaceID, request.NewParent, request.NewName,
	))
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
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
                    SELECT 1 FROM dofs_nodes
                    WHERE namespace_id = ? AND parent_inode = ? AND state IN ('creating', 'ready'))`,
				request.NamespaceID, destination.Inode,
			).Scan(&nonempty); err != nil {
				return dofs.Node{}, nil, err
			}
			if nonempty {
				return dofs.Node{}, nil, dofs.ErrDirectoryNotEmpty
			}
		}
		now := s.now().UTC()
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_nodes
            SET parent_inode = 0, state = 'deleted', deleted_at = ?, changed_at = ?
            WHERE namespace_id = ? AND inode = ?`,
			millis(now), millis(now), request.NamespaceID, destination.Inode,
		); err != nil {
			return dofs.Node{}, nil, err
		}
		if err := insertEvent(ctx, tx, dofs.Event{
			NamespaceID: destination.NamespaceID, Kind: dofs.EventRemove, Inode: destination.Inode,
			Generation: destination.Generation, OldParent: destination.Parent,
			OldName: destination.Name, CreatedAt: now,
		}); err != nil {
			return dofs.Node{}, nil, err
		}
		destination.Parent = 0
		destination.State = dofs.NodeStateDeleted
		destination.ChangedAt = now
		destination.DeletedAt = &now
		replaced = &destination
	} else if !errors.Is(destinationErr, dofs.ErrNotFound) {
		return dofs.Node{}, nil, destinationErr
	}

	oldParent, oldName := node.Parent, node.Name
	now := s.now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_nodes
        SET parent_inode = ?, name = ?, changed_at = ?
        WHERE namespace_id = ? AND inode = ?`,
		request.NewParent, request.NewName, millis(now), request.NamespaceID, node.Inode,
	); err != nil {
		return dofs.Node{}, nil, mapError(err)
	}
	node.Parent = request.NewParent
	node.Name = request.NewName
	node.ChangedAt = now
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: dofs.EventRename, Inode: node.Inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name,
		OldParent: oldParent, OldName: oldName, CreatedAt: now,
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
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes
        WHERE namespace_id = ? AND parent_inode = ? AND name = ? AND state = 'ready'`,
		request.NamespaceID, request.Parent, request.Name,
	))
	if err != nil {
		return dofs.Node{}, err
	}
	if node.IsDir() != request.Directory {
		return dofs.Node{}, dofs.ErrTypeMismatch
	}
	if node.IsDir() {
		var nonempty bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
                SELECT 1 FROM dofs_nodes
                WHERE namespace_id = ? AND parent_inode = ? AND state IN ('creating', 'ready'))`,
			request.NamespaceID, node.Inode,
		).Scan(&nonempty); err != nil {
			return dofs.Node{}, err
		}
		if nonempty {
			return dofs.Node{}, dofs.ErrDirectoryNotEmpty
		}
	}
	now := s.now().UTC()
	oldParent, oldName := node.Parent, node.Name
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_nodes
        SET parent_inode = 0, state = 'deleted', deleted_at = ?, changed_at = ?
        WHERE namespace_id = ? AND inode = ?`,
		millis(now), millis(now), request.NamespaceID, node.Inode,
	); err != nil {
		return dofs.Node{}, err
	}
	node.Parent = 0
	node.State = dofs.NodeStateDeleted
	node.ChangedAt = now
	node.DeletedAt = &now
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: node.NamespaceID, Kind: dofs.EventRemove, Inode: node.Inode,
		Generation: node.Generation, OldParent: oldParent, OldName: oldName, CreatedAt: now,
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
	if inode == 0 || inode > math.MaxInt64 {
		return false, nil
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := lockNamespace(ctx, tx, namespaceID); err != nil {
		return false, err
	}
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, namespaceID, inode))
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
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
            SELECT 1 FROM dofs_nodes WHERE namespace_id = ? AND parent_inode = ?)`, namespaceID, inode).Scan(&nonempty); err != nil {
			return false, err
		}
		if nonempty {
			return false, dofs.ErrDirectoryNotEmpty
		}
	}
	var used int64
	if err := tx.QueryRowContext(ctx, `SELECT used_bytes FROM dofs_namespaces WHERE id = ?`, namespaceID).Scan(&used); err != nil {
		return false, mapError(err)
	}
	if used < node.Size {
		return false, errors.New("dofs sqlite: corrupt namespace usage")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dofs_nodes WHERE namespace_id = ? AND inode = ?`, namespaceID, inode); err != nil {
		return false, fmt.Errorf("purge DOFS SQLite inode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_namespaces
        SET used_bytes = ?, updated_at = ? WHERE id = ?`, used-node.Size, millis(s.now()), namespaceID); err != nil {
		return false, fmt.Errorf("release DOFS SQLite namespace usage: %w", err)
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
	if inode == 0 || inode > math.MaxInt64 {
		return nil, dofs.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx, `SELECT namespace_id, inode, number, object_key, size, created_at
        FROM dofs_generations WHERE namespace_id = ? AND inode = ? ORDER BY number`, namespaceID, inode)
	if err != nil {
		return nil, fmt.Errorf("list DOFS SQLite generations: %w", err)
	}
	defer rows.Close()
	var result []dofs.Generation
	for rows.Next() {
		var generation dofs.Generation
		var inodeValue, createdAt int64
		if err := rows.Scan(
			&generation.NamespaceID, &inodeValue, &generation.Number,
			&generation.ObjectKey, &generation.Size, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan DOFS SQLite generation: %w", err)
		}
		if inodeValue <= 0 {
			return nil, errors.New("dofs sqlite: corrupt generation inode")
		}
		generation.Inode = uint64(inodeValue)
		generation.CreatedAt = fromMillis(createdAt)
		result = append(result, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list DOFS SQLite generations: %w", err)
	}
	if len(result) == 0 {
		if _, err := s.GetNode(ctx, namespaceID, inode); err != nil {
			return nil, err
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
	rows, err := s.db.QueryContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND state = ? ORDER BY inode`, namespaceID, state)
	if err != nil {
		return nil, fmt.Errorf("list DOFS SQLite inode state: %w", err)
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
	if inode == 0 || inode > math.MaxInt64 {
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
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+sqliteNodeColumns+`
        FROM dofs_nodes WHERE namespace_id = ? AND inode = ? AND state = 'ready'`, namespaceID, inode))
	if err != nil {
		return dofs.Node{}, err
	}
	now := s.now().UTC()
	node.Mode = mode & 0777
	node.ChangedAt = now
	if _, err := tx.ExecContext(ctx, `UPDATE dofs_nodes SET mode = ?, changed_at = ?
        WHERE namespace_id = ? AND inode = ?`, node.Mode, millis(now), namespaceID, inode); err != nil {
		return dofs.Node{}, err
	}
	if err := insertEvent(ctx, tx, dofs.Event{
		NamespaceID: namespaceID, Kind: dofs.EventChmod, Inode: inode,
		Generation: node.Generation, Parent: node.Parent, Name: node.Name, CreatedAt: now,
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
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, namespace_id, kind, inode, generation,
        parent_inode, name, old_parent_inode, old_name, created_at
        FROM dofs_events
        WHERE namespace_id = ? AND sequence > ? ORDER BY sequence LIMIT ?`, namespaceID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list DOFS SQLite events: %w", err)
	}
	defer rows.Close()
	var result []dofs.Event
	for rows.Next() {
		var event dofs.Event
		var inode, parent, oldParent, createdAt int64
		if err := rows.Scan(
			&event.Sequence, &event.NamespaceID, &event.Kind, &inode, &event.Generation,
			&parent, &event.Name, &oldParent, &event.OldName, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan DOFS SQLite event: %w", err)
		}
		if inode <= 0 || parent < 0 || oldParent < 0 {
			return nil, errors.New("dofs sqlite: corrupt event metadata")
		}
		event.Inode = uint64(inode)
		event.Parent = uint64(parent)
		event.OldParent = uint64(oldParent)
		event.CreatedAt = fromMillis(createdAt)
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *Store) LatestEventSequence(ctx context.Context, namespaceID string) (int64, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return 0, err
	}
	var sequence int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM dofs_events WHERE namespace_id = ?`, namespaceID).
		Scan(&sequence); err != nil {
		return 0, fmt.Errorf("read latest DOFS SQLite event sequence: %w", err)
	}
	return sequence, nil
}

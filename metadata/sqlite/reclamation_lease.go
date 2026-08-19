package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/willvar/dofs"
)

func (s *Store) AcquireMountLease(ctx context.Context, namespaceID string) (dofs.Lease, error) {
	return s.acquireReclamationLease(ctx, namespaceID, "mount")
}

func (s *Store) AcquireReclaimLease(ctx context.Context, namespaceID string) (dofs.Lease, error) {
	return s.acquireReclamationLease(ctx, namespaceID, "reclaim")
}

func (s *Store) acquireReclamationLease(ctx context.Context, namespaceID, mode string) (dofs.Lease, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if _, err := s.GetNamespace(ctx, namespaceID); err != nil {
		return nil, err
	}
	owner, err := newLeaseOwner()
	if err != nil {
		return nil, err
	}
	now := s.now().UTC()
	expiresAt := now.Add(s.leaseTTL)
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM dofs_reclamation_leases
        WHERE namespace_id = ? AND expires_at <= ?`, namespaceID, millis(now)); err != nil {
		return nil, mapError(err)
	}
	var busy bool
	query := `SELECT EXISTS(SELECT 1 FROM dofs_reclamation_leases WHERE namespace_id = ?)`
	if err := tx.QueryRowContext(ctx, query, namespaceID).Scan(&busy); err != nil {
		return nil, mapError(err)
	}
	if busy {
		if mode == "mount" {
			return nil, dofs.ErrMountBusy
		}
		return nil, dofs.ErrReclaimBusy
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dofs_reclamation_leases(
        namespace_id, owner, mode, acquired_at, expires_at
    ) VALUES (?, ?, ?, ?, ?)`, namespaceID, owner, mode, millis(now), millis(expiresAt)); err != nil {
		return nil, mapError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, mapError(err)
	}
	lease := &reclamationLease{
		store: s, namespaceID: namespaceID, owner: owner, mode: mode,
		lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go lease.monitor()
	return lease, nil
}

type reclamationLease struct {
	store       *Store
	namespaceID string
	owner       string
	mode        string

	mu       sync.Mutex
	err      error
	lost     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
	lostOnce sync.Once
}

func (l *reclamationLease) Lost() <-chan struct{} { return l.lost }

func (l *reclamationLease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *reclamationLease) monitor() {
	defer close(l.done)
	ticker := time.NewTicker(l.store.leaseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			if err := l.renew(); err != nil {
				l.markLost(err)
				return
			}
		}
	}
}

func (l *reclamationLease) renew() error {
	now := l.store.now().UTC()
	timeout := l.store.leaseHeartbeat
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result, err := l.store.db.ExecContext(ctx, `UPDATE dofs_reclamation_leases
        SET expires_at = ?
        WHERE namespace_id = ? AND owner = ? AND mode = ? AND expires_at > ?`,
		millis(now.Add(l.store.leaseTTL)), l.namespaceID, l.owner, l.mode, millis(now))
	if err != nil {
		return fmt.Errorf("%w: renew SQLite reclamation lease: %v", dofs.ErrLeaseLost, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: inspect SQLite reclamation lease renewal: %v", dofs.ErrLeaseLost, err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: SQLite reclamation lease expired or was replaced", dofs.ErrLeaseLost)
	}
	return nil
}

func (l *reclamationLease) markLost(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
	l.lostOnce.Do(func() { close(l.lost) })
}

func (l *reclamationLease) Close() error {
	if l == nil {
		return nil
	}
	l.close.Do(func() {
		close(l.stop)
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := l.store.db.ExecContext(ctx, `DELETE FROM dofs_reclamation_leases
            WHERE namespace_id = ? AND owner = ? AND mode = ?`, l.namespaceID, l.owner, l.mode)
		if err != nil {
			l.markLost(fmt.Errorf("%w: release SQLite reclamation lease: %v", dofs.ErrLeaseLost, err))
			return
		}
		affected, err := result.RowsAffected()
		if err != nil {
			l.markLost(fmt.Errorf("%w: inspect SQLite reclamation lease release: %v", dofs.ErrLeaseLost, err))
			return
		}
		if affected == 0 && l.Err() == nil {
			if _, namespaceErr := l.store.GetNamespace(ctx, l.namespaceID); errors.Is(namespaceErr, dofs.ErrNotFound) {
				return
			}
			l.markLost(fmt.Errorf("%w: SQLite reclamation lease was no longer held", dofs.ErrLeaseLost))
		}
	})
	return l.Err()
}

var _ dofs.Lease = (*reclamationLease)(nil)

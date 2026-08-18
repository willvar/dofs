package sqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/willvar/dofs"
)

func (s *Store) AcquireWriterLease(ctx context.Context, namespaceID string) (dofs.Lease, error) {
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return nil, err
	}
	if _, err := s.GetNamespace(ctx, namespaceID); err != nil {
		return nil, err
	}
	ownerBytes := make([]byte, 16)
	if _, err := rand.Read(ownerBytes); err != nil {
		return nil, fmt.Errorf("generate DOFS SQLite lease owner: %w", err)
	}
	owner := hex.EncodeToString(ownerBytes)
	now := s.now().UTC()
	expiresAt := now.Add(s.leaseTTL)
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO dofs_writer_leases(
        namespace_id, owner, acquired_at, expires_at
    ) VALUES (?, ?, ?, ?)
    ON CONFLICT(namespace_id) DO UPDATE SET
        owner = excluded.owner,
        acquired_at = excluded.acquired_at,
        expires_at = excluded.expires_at
    WHERE dofs_writer_leases.expires_at <= ?`,
		namespaceID, owner, millis(now), millis(expiresAt), millis(now),
	)
	if err != nil {
		return nil, mapError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, dofs.ErrWriterBusy
	}
	if err := tx.Commit(); err != nil {
		return nil, mapError(err)
	}
	lease := &writerLease{
		store: s, namespaceID: namespaceID, owner: owner,
		lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go lease.monitor()
	return lease, nil
}

type writerLease struct {
	store       *Store
	namespaceID string
	owner       string

	mu       sync.Mutex
	err      error
	lost     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
	lostOnce sync.Once
}

func (l *writerLease) Lost() <-chan struct{} { return l.lost }

func (l *writerLease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *writerLease) monitor() {
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

func (l *writerLease) renew() error {
	now := l.store.now().UTC()
	timeout := l.store.leaseHeartbeat
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result, err := l.store.db.ExecContext(ctx, `UPDATE dofs_writer_leases
        SET expires_at = ?
        WHERE namespace_id = ? AND owner = ? AND expires_at > ?`,
		millis(now.Add(l.store.leaseTTL)), l.namespaceID, l.owner, millis(now),
	)
	if err != nil {
		return fmt.Errorf("%w: renew SQLite writer lease: %v", dofs.ErrLeaseLost, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: inspect SQLite writer lease renewal: %v", dofs.ErrLeaseLost, err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: SQLite writer lease expired or was replaced", dofs.ErrLeaseLost)
	}
	return nil
}

func (l *writerLease) markLost(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
	l.lostOnce.Do(func() { close(l.lost) })
}

func (l *writerLease) Close() error {
	if l == nil {
		return nil
	}
	l.close.Do(func() {
		close(l.stop)
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := l.store.db.ExecContext(ctx, `DELETE FROM dofs_writer_leases
            WHERE namespace_id = ? AND owner = ?`, l.namespaceID, l.owner)
		if err != nil {
			l.markLost(fmt.Errorf("%w: release SQLite writer lease: %v", dofs.ErrLeaseLost, err))
			return
		}
		affected, err := result.RowsAffected()
		if err != nil {
			l.markLost(fmt.Errorf("%w: inspect SQLite writer lease release: %v", dofs.ErrLeaseLost, err))
			return
		}
		if affected == 0 && l.Err() == nil {
			l.markLost(fmt.Errorf("%w: SQLite writer lease was no longer held", dofs.ErrLeaseLost))
		}
	})
	return l.Err()
}

var _ dofs.Lease = (*writerLease)(nil)

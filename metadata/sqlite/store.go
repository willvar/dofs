// Package sqlite implements DOFS metadata in a private, local SQLite database.
// It is the default backend for a single-host deployment. The PostgreSQL
// adapter should be used when metadata must be shared by multiple hosts.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/willvar/dofs"
	sqlitedriver "modernc.org/sqlite"
)

const sqliteNodeColumns = `
namespace_id, inode, parent_inode, name, kind, state, mode, size, generation,
object_key, wrapped_dek, created_at, modified_at, changed_at, deleted_at`

type Config struct {
	Path               string
	BusyTimeout        time.Duration
	MaxOpenConnections int
}

type Store struct {
	db             *sql.DB
	path           string
	now            func() time.Time
	leaseTTL       time.Duration
	leaseHeartbeat time.Duration
}

// Open creates or opens a SQLite metadata database with WAL, full synchronous
// durability, foreign keys and immediate write transactions enabled.
func Open(ctx context.Context, config Config) (*Store, error) {
	if strings.TrimSpace(config.Path) == "" {
		return nil, errors.New("dofs sqlite: database path is required")
	}
	path, err := filepath.Abs(config.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve DOFS SQLite path: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, errors.New("dofs sqlite: database path must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect DOFS SQLite path: %w", err)
	}
	if err := ensurePrivateRealDirectory(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("protect DOFS SQLite directory: %w", err)
	}
	busyTimeout := config.BusyTimeout
	if busyTimeout == 0 {
		busyTimeout = 5 * time.Second
	}
	if busyTimeout < 0 {
		return nil, errors.New("dofs sqlite: busy timeout cannot be negative")
	}
	connections := config.MaxOpenConnections
	if connections == 0 {
		connections = 8
	}
	if connections < 1 {
		return nil, errors.New("dofs sqlite: max open connections must be positive")
	}
	dsn := sqliteDSN(path, busyTimeout)
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open DOFS SQLite database: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = database.Close()
		}
	}()
	if err := database.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connect DOFS SQLite database: %w", err)
	}
	var journalMode string
	if err := database.QueryRowContext(ctx, `PRAGMA journal_mode = WAL`).Scan(&journalMode); err != nil {
		return nil, fmt.Errorf("enable DOFS SQLite WAL: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return nil, fmt.Errorf("dofs sqlite: WAL unavailable (journal mode %q)", journalMode)
	}
	if _, err := database.ExecContext(ctx, `PRAGMA journal_size_limit = 67108864`); err != nil {
		return nil, fmt.Errorf("limit DOFS SQLite WAL: %w", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		return nil, fmt.Errorf("protect DOFS SQLite database: %w", err)
	}
	database.SetMaxOpenConns(connections)
	database.SetMaxIdleConns(connections)
	closeOnError = false
	return &Store{
		db: database, path: path, now: time.Now,
		leaseTTL: 15 * time.Second, leaseHeartbeat: 3 * time.Second,
	}, nil
}

// ensurePrivateRealDirectory rejects a state path containing a symbolic-link
// component. SQLite creates WAL/SHM siblings next to the database, so checking
// only the final database file would still allow those plaintext metadata
// files to be redirected through an attacker-controlled parent directory.
func ensurePrivateRealDirectory(directory string) error {
	directory = filepath.Clean(directory)
	if !filepath.IsAbs(directory) || directory == string(filepath.Separator) {
		return errors.New("directory must be an absolute non-root path")
	}
	volume := filepath.VolumeName(directory)
	current := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(directory, current)
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0700); err != nil {
				return err
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("path component %s must be a real directory", current)
		}
	}
	return os.Chmod(directory, 0700)
}

func sqliteDSN(path string, busyTimeout time.Duration) string {
	parameters := url.Values{}
	parameters.Set("_txlock", "immediate")
	parameters.Add("_pragma", "foreign_keys(1)")
	parameters.Add("_pragma", "busy_timeout("+strconv.FormatInt(busyTimeout.Milliseconds(), 10)+")")
	parameters.Add("_pragma", "synchronous(FULL)")
	parameters.Add("_pragma", "wal_autocheckpoint(1000)")
	parameters.Add("_pragma", "trusted_schema(OFF)")
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: parameters.Encode()}).String()
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	_, checkpointErr := s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	closeErr := s.db.Close()
	s.db = nil
	if checkpointErr != nil {
		return fmt.Errorf("checkpoint DOFS SQLite WAL: %w", checkpointErr)
	}
	return closeErr
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return fmt.Errorf("begin DOFS SQLite migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS dofs_schema (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        version   INTEGER NOT NULL CHECK (version >= 0)
    )`); err != nil {
		return fmt.Errorf("create DOFS SQLite schema table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO dofs_schema(singleton, version) VALUES (1, 0)`); err != nil {
		return fmt.Errorf("initialize DOFS SQLite schema: %w", err)
	}
	var current int
	if err := tx.QueryRowContext(ctx, `SELECT version FROM dofs_schema WHERE singleton = 1`).Scan(&current); err != nil {
		return fmt.Errorf("read DOFS SQLite schema version: %w", err)
	}
	if current > schemaVersion {
		return fmt.Errorf("DOFS SQLite schema %d is newer than supported version %d", current, schemaVersion)
	}
	if current < 1 {
		for _, statement := range schemaV1Statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("install DOFS SQLite schema v1: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 1 WHERE singleton = 1`); err != nil {
			return fmt.Errorf("record DOFS SQLite schema v1: %w", err)
		}
		current = 1
	}
	if current < 2 {
		for _, statement := range schemaV2Statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("install DOFS SQLite schema v2: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 2 WHERE singleton = 1`); err != nil {
			return fmt.Errorf("record DOFS SQLite schema v2: %w", err)
		}
		current = 2
	}
	if current < 3 {
		for _, statement := range schemaV3Statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("install DOFS SQLite schema v3: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 3 WHERE singleton = 1`); err != nil {
			return fmt.Errorf("record DOFS SQLite schema v3: %w", err)
		}
		current = 3
	}
	if current < 4 {
		for _, statement := range schemaV4Statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("install DOFS SQLite schema v4: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 4 WHERE singleton = 1`); err != nil {
			return fmt.Errorf("record DOFS SQLite schema v4: %w", err)
		}
		current = 4
	}
	if current < 5 {
		for _, statement := range schemaV5Statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("install DOFS SQLite schema v5: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dofs_schema SET version = 5 WHERE singleton = 1`); err != nil {
			return fmt.Errorf("record DOFS SQLite schema v5: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DOFS SQLite migration: %w", err)
	}
	var violationTable any
	if err := s.db.QueryRowContext(ctx, `SELECT "table" FROM pragma_foreign_key_check LIMIT 1`).Scan(&violationTable); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check DOFS SQLite foreign keys: %w", err)
	} else if err == nil {
		return fmt.Errorf("dofs sqlite: foreign key violation in table %v", violationTable)
	}
	return nil
}

func (s *Store) begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, mapError(err)
	}
	return tx, nil
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
	now := s.now().UTC()
	var liveLease int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dofs_writer_leases
        WHERE namespace_id = ? AND expires_at > ?`, id, millis(now)).Scan(&liveLease); err != nil {
		return err
	}
	if liveLease != 0 {
		return dofs.ErrWriterBusy
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dofs_reclamation_leases
        WHERE namespace_id = ? AND expires_at > ?`, id, millis(now)).Scan(&liveLease); err != nil {
		return err
	}
	if liveLease != 0 {
		return dofs.ErrReclaimBusy
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dofs_namespaces WHERE id = ?`, id)
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

type sqliteCodeError interface {
	Code() int
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return dofs.ErrNotFound
	}
	var sqliteErr *sqlitedriver.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case 1555, 2067:
			return dofs.ErrAlreadyExists
		case 5, 6, 261, 262, 517, 518:
			return fmt.Errorf("%w: SQLite metadata is busy", dofs.ErrConflict)
		}
	}
	var coded sqliteCodeError
	if errors.As(err, &coded) && (coded.Code() == 5 || coded.Code() == 6) {
		return fmt.Errorf("%w: SQLite metadata is busy", dofs.ErrConflict)
	}
	return err
}

func millis(value time.Time) int64 { return value.UTC().UnixMilli() }
func fromMillis(value int64) time.Time {
	return time.UnixMilli(value).UTC()
}

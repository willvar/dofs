package dofs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	walStateDirty     = "dirty"
	walStatePrepared  = "prepared"
	walStateUploaded  = "uploaded"
	walStateDiscarded = "discarded"
)

type walEntry struct {
	ID               string      `json:"id"`
	NamespaceID      string      `json:"namespace_id"`
	Inode            uint64      `json:"inode"`
	BaseGeneration   int64       `json:"base_generation"`
	TargetGeneration int64       `json:"target_generation"`
	TargetObjectKey  string      `json:"target_object_key"`
	Size             int64       `json:"size"`
	DirtyRanges      []byteRange `json:"dirty_ranges,omitempty"`
	State            string      `json:"state"`
	UpdatedAt        time.Time   `json:"updated_at"`
}

// WritebackManager owns one namespace's private sparse plaintext cache and
// crash-recovery WAL. WAL records contain no encryption keys.
type WritebackManager struct {
	backend  *Backend
	walDir   string
	cacheDir string
	lock     *stateDirectoryLock

	mu     sync.Mutex
	closed bool
}

func newWritebackManager(backend *Backend, stateDirectory string) (*WritebackManager, error) {
	if backend == nil || strings.TrimSpace(stateDirectory) == "" {
		return nil, errors.New("dofs: writeback state directory is required")
	}
	absolute, err := filepath.Abs(stateDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve writeback state directory: %w", err)
	}
	if err := ensurePrivateDirectory(absolute); err != nil {
		return nil, err
	}
	walDirectory := filepath.Join(absolute, "wal")
	cacheDirectory := filepath.Join(absolute, "cache")
	if err := ensurePrivateDirectory(walDirectory); err != nil {
		return nil, err
	}
	if err := ensurePrivateDirectory(cacheDirectory); err != nil {
		return nil, err
	}
	lock, err := acquireStateDirectoryLock(absolute)
	if err != nil {
		return nil, err
	}
	return &WritebackManager{backend: backend, walDir: walDirectory, cacheDir: cacheDirectory, lock: lock}, nil
}

func ensurePrivateDirectory(directory string) error {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return fmt.Errorf("create private directory %s: %w", directory, err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect private directory %s: %w", directory, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("private path %s must be a real directory", directory)
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return fmt.Errorf("resolve private directory %s: %w", directory, err)
	}
	if filepath.Clean(resolved) != filepath.Clean(directory) {
		return fmt.Errorf("private path %s must not contain symbolic-link components", directory)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return fmt.Errorf("protect private directory %s: %w", directory, err)
	}
	return nil
}

func (m *WritebackManager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	lock := m.lock
	m.lock = nil
	m.mu.Unlock()
	if lock != nil {
		_ = lock.Close()
	}
}

func (m *WritebackManager) operationalError() error {
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return ErrHandleClosed
	}
	if lost := m.backend.LeaseLost(); lost != nil {
		select {
		case <-lost:
			if err := m.backend.LeaseError(); err != nil {
				return fmt.Errorf("%w: %v", ErrLeaseLost, err)
			}
			return ErrLeaseLost
		default:
		}
	}
	return nil
}

func (m *WritebackManager) walPath(id string) string   { return filepath.Join(m.walDir, id+".json") }
func (m *WritebackManager) cachePath(id string) string { return filepath.Join(m.cacheDir, id+".cache") }

func validTransactionID(id string) bool {
	if len(id) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16
}

func (m *WritebackManager) validateEntry(entry *walEntry) error {
	if entry == nil || !validTransactionID(entry.ID) {
		return errors.New("invalid WAL transaction ID")
	}
	if entry.NamespaceID != m.backend.namespace.ID || entry.Inode == 0 || entry.BaseGeneration < 0 || entry.Size < 0 {
		return errors.New("WAL entry is outside this namespace")
	}
	if entry.TargetGeneration != entry.BaseGeneration+1 {
		return errors.New("invalid WAL generation transition")
	}
	wantPrefix, err := InodeObjectRoot(entry.NamespaceID, entry.Inode)
	if err != nil || !strings.HasPrefix(entry.TargetObjectKey, wantPrefix) {
		return errors.New("invalid WAL target object key")
	}
	previousEnd := int64(-1)
	for _, dirty := range entry.DirtyRanges {
		if dirty.Start < 0 || dirty.End <= dirty.Start || (previousEnd >= 0 && dirty.Start <= previousEnd) {
			return errors.New("invalid WAL dirty ranges")
		}
		previousEnd = dirty.End
	}
	switch entry.State {
	case walStateDirty, walStatePrepared, walStateUploaded, walStateDiscarded:
		return nil
	default:
		return errors.New("invalid WAL state")
	}
}

func (m *WritebackManager) persist(entry *walEntry) error {
	if err := m.validateEntry(entry); err != nil {
		return err
	}
	entry.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("encode WAL: %w", err)
	}
	suffix, err := randomTransactionID()
	if err != nil {
		return err
	}
	temporary := filepath.Join(m.walDir, entry.ID+"."+suffix+".tmp")
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create WAL: %w", err)
	}
	removeTemporary := true
	defer func() {
		_ = file.Close()
		if removeTemporary {
			_ = os.Remove(temporary)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	if err := os.Rename(temporary, m.walPath(entry.ID)); err != nil {
		return fmt.Errorf("publish WAL: %w", err)
	}
	removeTemporary = false
	return syncDirectory(m.walDir)
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func (m *WritebackManager) removeLocal(entry *walEntry, removeCache bool) error {
	if err := m.validateEntry(entry); err != nil {
		return err
	}
	if err := os.Remove(m.walPath(entry.ID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if removeCache {
		if err := os.Remove(m.cachePath(entry.ID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDirectory(m.cacheDir); err != nil {
			return err
		}
	}
	return syncDirectory(m.walDir)
}

func deleteObject(objects ObjectStore, ctx context.Context, key string) error {
	err := objects.Delete(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		return nil
	}
	return err
}

// Recover resumes only transactions that reached prepared/fsync. Dirty cache
// entries had no durability promise and are discarded.
func (m *WritebackManager) Recover(ctx context.Context) error {
	entries, err := os.ReadDir(m.walDir)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(first, second int) bool { return entries[first].Name() < entries[second].Name() })
	activeCaches := make(map[string]struct{})
	for _, directoryEntry := range entries {
		if directoryEntry.IsDir() {
			continue
		}
		name := directoryEntry.Name()
		if strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(filepath.Join(m.walDir, name))
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		id := strings.TrimSuffix(name, ".json")
		if !validTransactionID(id) {
			return fmt.Errorf("unexpected WAL file %q", name)
		}
		info, err := directoryEntry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 1024*1024 {
			return fmt.Errorf("invalid WAL file %q", name)
		}
		data, err := os.ReadFile(m.walPath(id))
		if err != nil {
			return err
		}
		var entry walEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return fmt.Errorf("decode WAL %s: %w", id, err)
		}
		if entry.ID != id {
			return fmt.Errorf("WAL filename does not match transaction %s", entry.ID)
		}
		if err := m.validateEntry(&entry); err != nil {
			return fmt.Errorf("validate WAL %s: %w", id, err)
		}
		cacheName := id + ".cache"
		activeCaches[cacheName] = struct{}{}
		if entry.State == walStateDirty {
			if err := m.removeLocal(&entry, true); err != nil {
				return err
			}
			delete(activeCaches, cacheName)
			continue
		}
		if entry.State == walStateDiscarded {
			if err := deleteObject(m.backend.objects, ctx, entry.TargetObjectKey); err != nil {
				return fmt.Errorf("clean discarded WAL object %s: %w", id, err)
			}
			if err := m.removeLocal(&entry, true); err != nil {
				return err
			}
			delete(activeCaches, cacheName)
			continue
		}

		node, err := m.backend.metadata.GetNode(ctx, entry.NamespaceID, entry.Inode)
		if errors.Is(err, ErrNotFound) {
			if entry.State == walStateUploaded {
				_ = deleteObject(m.backend.objects, ctx, entry.TargetObjectKey)
			}
			if err := m.removeLocal(&entry, true); err != nil {
				return err
			}
			delete(activeCaches, cacheName)
			continue
		}
		if err != nil {
			return err
		}
		if node.Generation == entry.TargetGeneration && node.ObjectKey == entry.TargetObjectKey {
			if err := m.removeLocal(&entry, true); err != nil {
				return err
			}
			delete(activeCaches, cacheName)
			continue
		}
		if node.Generation != entry.BaseGeneration {
			if err := deleteObject(m.backend.objects, ctx, entry.TargetObjectKey); err != nil {
				return fmt.Errorf("clean stale WAL object %s: %w", id, err)
			}
			if err := m.removeLocal(&entry, true); err != nil {
				return err
			}
			delete(activeCaches, cacheName)
			continue
		}
		cache, err := openRegularFile(m.cachePath(id))
		if err != nil {
			return fmt.Errorf("open WAL cache %s: %w", id, err)
		}
		session := &WriteSession{
			manager: m, cache: cache, cachePath: m.cachePath(id), node: node,
			entry: &entry, walReady: true, size: entry.Size,
			dirtyRanges:  append([]byteRange(nil), entry.DirtyRanges...),
			loadedRanges: append([]byteRange(nil), entry.DirtyRanges...),
		}
		cacheInfo, statErr := cache.Stat()
		if statErr != nil || cacheInfo.Size() != entry.Size {
			_ = cache.Close()
			if statErr != nil {
				return statErr
			}
			return fmt.Errorf("WAL cache %s has size %d, want %d", id, cacheInfo.Size(), entry.Size)
		}
		err = session.commitLocked(ctx)
		_ = cache.Close()
		if err != nil {
			return fmt.Errorf("replay WAL %s: %w", id, err)
		}
		_ = os.Remove(session.cachePath)
		delete(activeCaches, cacheName)
	}

	cacheEntries, err := os.ReadDir(m.cacheDir)
	if err != nil {
		return err
	}
	for _, entry := range cacheEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".cache") {
			continue
		}
		if _, active := activeCaches[entry.Name()]; active {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".cache")
		if validTransactionID(id) {
			_ = os.Remove(filepath.Join(m.cacheDir, entry.Name()))
		}
	}
	return nil
}

func openRegularFile(filePath string) (*os.File, error) {
	info, err := os.Lstat(filePath)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("cache path is not a regular file")
	}
	return os.OpenFile(filePath, os.O_RDWR, 0600)
}

// WriteSession is an isolated plaintext snapshot for one inode writer. The
// FUSE adapter shares it across concurrent handles for that inode; independent
// backend callers are still protected by the generation CAS during Commit.
type WriteSession struct {
	manager      *WritebackManager
	cache        *os.File
	cachePath    string
	node         Node
	entry        *walEntry
	walReady     bool
	onCommit     func(Node)
	size         int64
	loadedRanges []byteRange
	dirtyRanges  []byteRange
	terminalErr  error

	mu     sync.Mutex
	closed bool
}

func (m *WritebackManager) open(ctx context.Context, node Node, onCommit func(Node)) (*WriteSession, error) {
	if err := m.operationalError(); err != nil {
		return nil, err
	}
	if node.Inode == 0 || !node.IsFile() {
		return nil, errors.New("invalid file for writeback")
	}
	current, err := m.backend.metadata.GetNode(ctx, m.backend.namespace.ID, node.Inode)
	if err != nil {
		return nil, err
	}
	if !m.backend.validNode(current, true) || current.Size < 0 || len(current.WrappedDEK) == 0 {
		return nil, ErrNotFound
	}
	id, err := randomTransactionID()
	if err != nil {
		return nil, err
	}
	cachePath := m.cachePath(id)
	cache, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("create plaintext cache: %w", err)
	}
	session := &WriteSession{manager: m, cache: cache, cachePath: cachePath, node: current, size: current.Size, onCommit: onCommit}
	if err := cache.Truncate(current.Size); err != nil {
		_ = cache.Close()
		_ = os.Remove(cachePath)
		return nil, err
	}
	return session, nil
}

func (s *WriteSession) beginTransactionLocked() error {
	if s.entry != nil {
		if !s.walReady {
			if err := s.manager.persist(s.entry); err != nil {
				return err
			}
			s.walReady = true
		}
		return nil
	}
	transactionID, err := randomTransactionID()
	if err != nil {
		return err
	}
	newCachePath := s.manager.cachePath(transactionID)
	if err := os.Rename(s.cachePath, newCachePath); err != nil {
		return fmt.Errorf("rotate plaintext cache: %w", err)
	}
	s.cachePath = newCachePath
	objectKey, err := GenerationObjectKey(s.node.NamespaceID, s.node.Inode, s.node.Generation+1, transactionID)
	if err != nil {
		return err
	}
	s.entry = &walEntry{
		ID: transactionID, NamespaceID: s.node.NamespaceID, Inode: s.node.Inode,
		BaseGeneration: s.node.Generation, TargetGeneration: s.node.Generation + 1,
		TargetObjectKey: objectKey, Size: s.size, State: walStateDirty,
	}
	if err := s.manager.persist(s.entry); err != nil {
		return err
	}
	s.walReady = true
	return nil
}

func (s *WriteSession) ReadAt(ctx context.Context, destination []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrHandleClosed
	}
	if s.terminalErr != nil {
		return 0, s.terminalErr
	}
	if offset < 0 {
		return 0, errors.New("negative read offset")
	}
	if len(destination) == 0 {
		return 0, nil
	}
	if offset >= s.size {
		return 0, io.EOF
	}
	wanted := int64(len(destination))
	if remaining := s.size - offset; wanted > remaining {
		wanted = remaining
	}
	baseEnd := min(offset+wanted, s.node.Size)
	for _, missing := range missingByteRanges(s.loadedRanges, offset, baseEnd) {
		plaintext, err := s.manager.backend.ReadAt(ctx, s.node, missing.Start, int(missing.End-missing.Start))
		if err != nil {
			return 0, fmt.Errorf("fill sparse plaintext cache: %w", err)
		}
		written, err := s.cache.WriteAt(plaintext, missing.Start)
		if err != nil {
			return 0, err
		}
		if written != len(plaintext) {
			return 0, io.ErrShortWrite
		}
		s.loadedRanges = addByteRange(s.loadedRanges, missing.Start, missing.End)
	}
	read, err := s.cache.ReadAt(destination[:wanted], offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return read, err
	}
	if int64(len(destination)) > wanted {
		return read, io.EOF
	}
	return read, nil
}

func (s *WriteSession) WriteAt(ctx context.Context, data []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrHandleClosed
	}
	if s.terminalErr != nil {
		return 0, s.terminalErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.manager.operationalError(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("negative write offset")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if err := s.beginTransactionLocked(); err != nil {
		return 0, err
	}
	written, err := s.cache.WriteAt(data, offset)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if written > 0 {
		end := offset + int64(written)
		s.loadedRanges = addByteRange(s.loadedRanges, offset, end)
		s.dirtyRanges = addByteRange(s.dirtyRanges, offset, end)
		if end > s.size {
			s.size = end
		}
	}
	return written, err
}

func (s *WriteSession) Truncate(ctx context.Context, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrHandleClosed
	}
	if s.terminalErr != nil {
		return s.terminalErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.manager.operationalError(); err != nil {
		return err
	}
	if size < 0 {
		return errors.New("negative truncate size")
	}
	if size == s.size {
		return nil
	}
	if err := s.beginTransactionLocked(); err != nil {
		return err
	}
	oldSize := s.size
	if err := s.cache.Truncate(size); err != nil {
		return err
	}
	if size < oldSize {
		// Preserve a zero override beyond the temporary EOF. A later grow in the
		// same transaction must not resurrect bytes from the base generation.
		s.loadedRanges = addByteRange(s.loadedRanges, size, oldSize)
		s.dirtyRanges = addByteRange(s.dirtyRanges, size, oldSize)
	}
	s.size = size
	return nil
}

func (s *WriteSession) Size() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrHandleClosed
	}
	if s.terminalErr != nil {
		return 0, s.terminalErr
	}
	return s.size, nil
}

func (s *WriteSession) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrHandleClosed
	}
	if s.terminalErr != nil {
		return s.terminalErr
	}
	return s.commitLocked(ctx)
}

func (s *WriteSession) commitLocked(ctx context.Context) error {
	if s.terminalErr != nil {
		return s.terminalErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.manager.operationalError(); err != nil {
		return err
	}
	if s.entry == nil {
		return nil
	}
	entry := s.entry
	if entry.State == walStateDiscarded {
		return s.discardLocked(ctx, entry, ErrWriteConflict)
	}
	if !s.walReady {
		if err := s.manager.persist(entry); err != nil {
			return err
		}
		s.walReady = true
	}
	if entry.State == walStateDirty {
		if err := s.cache.Sync(); err != nil {
			return fmt.Errorf("sync plaintext cache: %w", err)
		}
		prepared := *entry
		prepared.Size = s.size
		prepared.DirtyRanges = append([]byteRange(nil), s.dirtyRanges...)
		prepared.State = walStatePrepared
		if err := s.manager.persist(&prepared); err != nil {
			return err
		}
		*entry = prepared
	}
	if entry.State == walStatePrepared {
		dek, _, err := s.manager.backend.fileKeyAndChunkSize(ctx, s.node)
		if err != nil {
			return err
		}
		err = s.uploadEncrypted(ctx, dek, entry)
		Clear(dek)
		if err != nil {
			return err
		}
		uploaded := *entry
		uploaded.State = walStateUploaded
		if err := s.manager.persist(&uploaded); err != nil {
			return err
		}
		*entry = uploaded
	}
	if err := s.manager.operationalError(); err != nil {
		return err
	}
	current, switched, err := s.manager.backend.metadata.CommitGeneration(ctx, CommitGenerationRequest{
		NamespaceID: entry.NamespaceID, Inode: entry.Inode,
		ExpectedGeneration: entry.BaseGeneration, ObjectKey: entry.TargetObjectKey, Size: entry.Size,
	})
	if errors.Is(err, ErrQuotaExceeded) {
		return s.discardLocked(ctx, entry, ErrQuotaExceeded)
	}
	if err != nil {
		return fmt.Errorf("commit file generation: %w", err)
	}
	if !switched && (current.Generation != entry.TargetGeneration || current.ObjectKey != entry.TargetObjectKey) {
		return s.discardLocked(ctx, entry, ErrWriteConflict)
	}
	s.node = current
	if err := s.manager.removeLocal(entry, false); err != nil {
		return fmt.Errorf("remove committed WAL: %w", err)
	}
	s.entry = nil
	s.walReady = false
	s.size = current.Size
	s.loadedRanges = nil
	s.dirtyRanges = nil
	if err := s.cache.Truncate(0); err != nil {
		return fmt.Errorf("clear committed plaintext cache: %w", err)
	}
	if err := s.cache.Truncate(current.Size); err != nil {
		return fmt.Errorf("resize sparse plaintext cache: %w", err)
	}
	if s.onCommit != nil {
		s.onCommit(current)
	}
	return nil
}

func (s *WriteSession) discardLocked(ctx context.Context, entry *walEntry, cause error) error {
	if entry.State != walStateDiscarded {
		discarded := *entry
		discarded.State = walStateDiscarded
		if err := s.manager.persist(&discarded); err != nil {
			return fmt.Errorf("%w (persist discard state: %v)", cause, err)
		}
		*entry = discarded
	}
	if err := deleteObject(s.manager.backend.objects, ctx, entry.TargetObjectKey); err != nil {
		return fmt.Errorf("%w (object cleanup failed: %v)", cause, err)
	}
	if err := s.manager.removeLocal(entry, false); err != nil {
		return fmt.Errorf("%w (local cleanup failed: %v)", cause, err)
	}
	s.entry = nil
	s.walReady = false
	s.terminalErr = cause
	return cause
}

func (s *WriteSession) uploadEncrypted(ctx context.Context, dek []byte, entry *walEntry) error {
	plaintext, err := newSessionPlainReader(ctx, s, entry.Size)
	if err != nil {
		return err
	}
	reader, writer := io.Pipe()
	encryptResult := make(chan error, 1)
	go func() {
		defer plaintext.Close()
		err := EncryptStream(dek, plaintext, writer)
		_ = writer.CloseWithError(err)
		encryptResult <- err
	}()
	uploadErr := s.manager.backend.objects.Put(ctx, entry.TargetObjectKey, reader, EncryptedSize(entry.Size, DefaultChunkSize))
	_ = reader.CloseWithError(uploadErr)
	encryptErr := <-encryptResult
	if encryptErr != nil {
		return fmt.Errorf("encrypt generation: %w", encryptErr)
	}
	if uploadErr != nil && !errors.Is(uploadErr, ErrAlreadyExists) {
		return fmt.Errorf("upload generation: %w", uploadErr)
	}
	return nil
}

type sessionPlainReader struct {
	session   *WriteSession
	base      io.ReadCloser
	baseUntil int64
	offset    int64
	remaining int64
}

func newSessionPlainReader(ctx context.Context, session *WriteSession, size int64) (*sessionPlainReader, error) {
	reader := &sessionPlainReader{session: session, remaining: size}
	baseEnd := min(size, session.node.Size)
	missing := missingByteRanges(session.dirtyRanges, 0, baseEnd)
	if len(missing) == 0 {
		return reader, nil
	}
	reader.baseUntil = missing[len(missing)-1].End
	base, err := session.manager.backend.OpenPlaintext(ctx, session.node)
	if err != nil {
		return nil, fmt.Errorf("stream base generation: %w", err)
	}
	reader.base = base
	return reader, nil
}

func (r *sessionPlainReader) Read(destination []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(destination)) > r.remaining {
		destination = destination[:r.remaining]
	}
	wanted := int64(len(destination))
	clear(destination)
	if r.offset < r.baseUntil {
		baseBytes := min(wanted, r.baseUntil-r.offset)
		read, err := io.ReadFull(r.base, destination[:baseBytes])
		if err != nil {
			return read, fmt.Errorf("read base generation: %w", err)
		}
	}
	end := r.offset + wanted
	for _, dirty := range r.session.dirtyRanges {
		if dirty.End <= r.offset {
			continue
		}
		if dirty.Start >= end {
			break
		}
		start := max(dirty.Start, r.offset)
		dirtyEnd := min(dirty.End, end)
		read, err := r.session.cache.ReadAt(destination[start-r.offset:dirtyEnd-r.offset], start)
		if err != nil {
			return read, fmt.Errorf("read dirty plaintext cache: %w", err)
		}
		if int64(read) != dirtyEnd-start {
			return read, io.ErrUnexpectedEOF
		}
	}
	r.offset = end
	r.remaining -= wanted
	return int(wanted), nil
}

func (r *sessionPlainReader) Close() error {
	if r.base == nil {
		return nil
	}
	err := r.base.Close()
	r.base = nil
	return err
}

// Close commits pending writes. Commit failures retain prepared cache and WAL
// for recovery; successful and clean sessions remove plaintext state.
func (s *WriteSession) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := s.commitLocked(ctx); err != nil {
		_ = s.cache.Close()
		s.closed = true
		if (errors.Is(err, ErrWriteConflict) || errors.Is(err, ErrQuotaExceeded)) && s.entry == nil {
			_ = os.Remove(s.cachePath)
		}
		return err
	}
	if err := s.cache.Close(); err != nil {
		s.closed = true
		return err
	}
	s.closed = true
	if err := os.Remove(s.cachePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *WriteSession) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	_ = s.cache.Close()
	if s.entry != nil {
		if s.entry.State == walStateUploaded {
			_ = deleteObject(s.manager.backend.objects, context.Background(), s.entry.TargetObjectKey)
		}
		_ = s.manager.removeLocal(s.entry, false)
	}
	_ = os.Remove(s.cachePath)
	s.closed = true
}

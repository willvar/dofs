package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/willvar/dofs"
)

func TestSQLiteMetadataContractAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	store := openTestStore(t, path)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permission := info.Mode().Perm(); permission != 0600 {
		t.Fatalf("database mode = %o, want 600", permission)
	}

	namespaceID := "tenant_a"
	namespace, err := store.CreateNamespace(t.Context(), dofs.Namespace{
		ID: namespaceID, Label: "SQLite contract", WrappedKEK: []byte("wrapped-kek"), QuotaBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if namespace.UsedBytes != 0 || namespace.QuotaBytes != 16 {
		t.Fatalf("namespace = %#v", namespace)
	}
	root, err := store.GetNode(t.Context(), namespaceID, dofs.RootInode)
	if err != nil || !root.IsDir() || root.Mode != 0700 || root.Parent != 0 {
		t.Fatalf("root = %#v, %v", root, err)
	}
	directory, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespaceID, Parent: root.Inode, Name: "docs", Kind: dofs.NodeKindDirectory, Mode: 0750,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := createReadyFile(t, store, namespaceID, directory.Inode, "one.txt", "one-v1", 7)
	if _, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespaceID, Inode: file.Inode, ExpectedGeneration: 0,
		ObjectKey: generationKey(t, namespaceID, file.Inode, 1, "stale"), Size: 1,
	}); err != nil || ok {
		t.Fatalf("stale generation commit = %v, %v", ok, err)
	}
	if _, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespaceID, Inode: file.Inode, ExpectedGeneration: 1,
		ObjectKey: generationKey(t, namespaceID, file.Inode, 2, "too-large"), Size: 17,
	}); !errors.Is(err, dofs.ErrQuotaExceeded) || ok {
		t.Fatalf("quota commit = %v, %v", ok, err)
	}
	if _, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespaceID, Inode: file.Inode, ExpectedGeneration: 1,
		ObjectKey: generationKey(t, namespaceID, file.Inode+1, 2, "cross-inode"), Size: 7,
	}); !errors.Is(err, dofs.ErrInvalidObjectKey) || ok {
		t.Fatalf("cross-inode object reference = %v, %v", ok, err)
	}
	second := createReadyFile(t, store, namespaceID, directory.Inode, "two.txt", "two-v1", 3)
	renamed, replaced, err := store.Rename(t.Context(), dofs.RenameRequest{
		NamespaceID: namespaceID, Inode: file.Inode, NewParent: directory.Inode, NewName: second.Name,
	})
	if err != nil || renamed.Name != "two.txt" || replaced == nil || replaced.Inode != second.Inode {
		t.Fatalf("rename replace = %#v, %#v, %v", renamed, replaced, err)
	}
	events, err := store.ListEvents(t.Context(), namespaceID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 || events[3].Kind != dofs.EventRemove || events[3].Inode != second.Inode || events[4].Kind != dofs.EventRename {
		t.Fatalf("events = %#v", events)
	}
	removed, err := store.Remove(t.Context(), dofs.RemoveRequest{
		NamespaceID: namespaceID, Parent: directory.Inode, Name: renamed.Name,
	})
	if err != nil || removed.State != dofs.NodeStateDeleted {
		t.Fatalf("remove = %#v, %v", removed, err)
	}
	for _, inode := range []uint64{second.Inode, file.Inode} {
		purged, err := store.Purge(t.Context(), namespaceID, inode)
		if err != nil || !purged {
			t.Fatalf("purge inode %d = %v, %v", inode, purged, err)
		}
	}
	current, err := store.GetNamespace(t.Context(), namespaceID)
	if err != nil || current.UsedBytes != 0 {
		t.Fatalf("namespace after purge = %#v, %v", current, err)
	}
	testExternalUploadReservation(t, store, namespaceID, directory.Inode)
	testConcurrentNameReservation(t, store, namespaceID, directory.Inode)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestStore(t, path)
	if err := reopened.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if node, err := reopened.Lookup(t.Context(), namespaceID, directory.Inode, "contended"); err != nil || !node.IsDir() {
		t.Fatalf("persisted node = %#v, %v", node, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func testExternalUploadReservation(t *testing.T, store *Store, namespaceID string, parent uint64) {
	t.Helper()
	upload, err := store.PrepareExternalUpload(t.Context(), dofs.PrepareExternalUploadRequest{
		ID: "sqlite-direct", NamespaceID: namespaceID, Parent: parent, Name: "direct.bin",
		Size: 4, WrappedDEK: []byte("direct-wrapped-key"), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !upload.CreatesNode || upload.TargetGeneration != 1 {
		t.Fatalf("upload = %#v", upload)
	}
	if _, err := store.Lookup(t.Context(), namespaceID, parent, "direct.bin"); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("reserved node is visible: %v", err)
	}
	active, err := store.ListExternalUploads(t.Context(), namespaceID, dofs.ExternalUploadActive)
	if err != nil || len(active) != 1 || active[0].ID != upload.ID {
		t.Fatalf("active uploads = %#v, %v", active, err)
	}
	refreshedUntil := time.Now().Add(2 * time.Hour)
	refreshed, err := store.RefreshExternalUpload(t.Context(), namespaceID, upload.ID, refreshedUntil)
	if err != nil || refreshed.ExpiresAt.Before(refreshedUntil.Add(-time.Millisecond)) {
		t.Fatalf("refresh external upload = %#v, %v", refreshed, err)
	}
	node, committed, err := store.CommitExternalUpload(t.Context(), namespaceID, upload.ID)
	if err != nil || !committed || node.Generation != 1 || string(node.WrappedDEK) != "direct-wrapped-key" {
		t.Fatalf("commit external upload = %#v, %v, %v", node, committed, err)
	}
	if retry, committed, err := store.CommitExternalUpload(t.Context(), namespaceID, upload.ID); err != nil || committed || retry.Inode != node.Inode {
		t.Fatalf("retry external upload = %#v, %v, %v", retry, committed, err)
	}
	if err := store.AcknowledgeExternalUpload(t.Context(), namespaceID, upload.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Remove(t.Context(), dofs.RemoveRequest{
		NamespaceID: namespaceID, Parent: parent, Name: node.Name,
	}); err != nil {
		t.Fatal(err)
	}
	if purged, err := store.Purge(t.Context(), namespaceID, node.Inode); err != nil || !purged {
		t.Fatalf("purge direct upload = %v, %v", purged, err)
	}

	aborted, err := store.PrepareExternalUpload(t.Context(), dofs.PrepareExternalUploadRequest{
		ID: "sqlite-abort", NamespaceID: namespaceID, Parent: parent, Name: "aborted.bin",
		WrappedDEK: []byte("abort-key"), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AbortExternalUpload(t.Context(), namespaceID, aborted.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetNode(t.Context(), namespaceID, aborted.Inode); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("aborted inode still exists: %v", err)
	}
}

func TestSQLiteExpiredExternalUploadCannotCommitOrRefresh(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "metadata.sqlite"))
	defer store.Close()
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 18, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.CreateNamespace(t.Context(), dofs.Namespace{ID: "expiry", WrappedKEK: []byte("wrapped")}); err != nil {
		t.Fatal(err)
	}
	upload, err := store.PrepareExternalUpload(t.Context(), dofs.PrepareExternalUploadRequest{
		ID: "expired", NamespaceID: "expiry", Parent: dofs.RootInode, Name: "expired.bin",
		WrappedDEK: []byte("key"), ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, _, err := store.CommitExternalUpload(t.Context(), "expiry", upload.ID); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("expired commit error = %v", err)
	}
	if _, err := store.RefreshExternalUpload(t.Context(), "expiry", upload.ID, now.Add(time.Hour)); !errors.Is(err, dofs.ErrConflict) {
		t.Fatalf("expired refresh error = %v", err)
	}
	if _, err := store.AbortExternalUpload(t.Context(), "expiry", upload.ID); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteWriterLeaseAcrossStoresAndLossDetection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	first := openTestStore(t, path)
	defer first.Close()
	if err := first.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := first.CreateNamespace(t.Context(), dofs.Namespace{
		ID: "tenant", WrappedKEK: []byte("wrapped"),
	}); err != nil {
		t.Fatal(err)
	}
	second := openTestStore(t, path)
	defer second.Close()
	if err := second.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	first.leaseHeartbeat = 20 * time.Millisecond
	first.leaseTTL = 200 * time.Millisecond
	second.leaseHeartbeat = 20 * time.Millisecond
	second.leaseTTL = 200 * time.Millisecond

	lease, err := first.AcquireWriterLease(t.Context(), "tenant")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.AcquireWriterLease(t.Context(), "tenant"); !errors.Is(err, dofs.ErrWriterBusy) {
		t.Fatalf("second writer lease = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err = second.AcquireWriterLease(t.Context(), "tenant")
	if err != nil {
		t.Fatalf("lease after release: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}

	lostLease, err := first.AcquireWriterLease(t.Context(), "tenant")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.db.ExecContext(t.Context(), `UPDATE dofs_writer_leases
        SET owner = 'replacement', expires_at = ? WHERE namespace_id = 'tenant'`, millis(second.now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lostLease.Lost():
		if !errors.Is(lostLease.Err(), dofs.ErrLeaseLost) {
			t.Fatalf("lease loss error = %v", lostLease.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("writer lease loss was not detected")
	}
	if err := lostLease.Close(); !errors.Is(err, dofs.ErrLeaseLost) {
		t.Fatalf("close lost lease = %v", err)
	}

	if _, err := second.db.ExecContext(t.Context(), `UPDATE dofs_writer_leases
		SET acquired_at = ?, expires_at = ? WHERE namespace_id = 'tenant'`,
		millis(second.now().Add(-2*time.Second)), millis(second.now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	recovered, err := second.AcquireWriterLease(t.Context(), "tenant")
	if err != nil {
		t.Fatalf("replace expired lease: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDeleteNamespaceRequiresReleasedWriterLease(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "metadata.sqlite"))
	defer store.Close()
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateNamespace(t.Context(), dofs.Namespace{ID: "delete-me", WrappedKEK: []byte("wrapped")}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireWriterLease(t.Context(), "delete-me")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNamespace(t.Context(), "delete-me"); !errors.Is(err, dofs.ErrWriterBusy) {
		t.Fatalf("delete mounted namespace error = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNamespace(t.Context(), "delete-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetNamespace(t.Context(), "delete-me"); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("deleted namespace = %v", err)
	}
}

func TestSQLiteRejectsSymlinkedStateDirectory(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	linkedDirectory := filepath.Join(root, "linked")
	if err := os.Symlink(realDirectory, linkedDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), Config{Path: filepath.Join(linkedDirectory, "metadata.sqlite")}); err == nil ||
		!strings.Contains(err.Error(), "must be a real directory") {
		t.Fatalf("symlinked SQLite parent error = %v", err)
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(t.Context(), Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func createReadyFile(t *testing.T, store *Store, namespaceID string, parent uint64, name, transaction string, size int64) dofs.Node {
	t.Helper()
	file, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespaceID, Parent: parent, Name: name, Kind: dofs.NodeKindFile,
		WrappedDEK: []byte("wrapped-dek"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Lookup(t.Context(), namespaceID, parent, name); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("creating file is visible: %v", err)
	}
	file, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespaceID, Inode: file.Inode, ExpectedGeneration: 0,
		ObjectKey: generationKey(t, namespaceID, file.Inode, 1, transaction), Size: size,
	})
	if err != nil || !ok {
		t.Fatalf("commit %q = %v, %v", name, ok, err)
	}
	return file
}

func generationKey(t *testing.T, namespaceID string, inode uint64, generation int64, transaction string) string {
	t.Helper()
	key, err := dofs.GenerationObjectKey(namespaceID, inode, generation, transaction)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func testConcurrentNameReservation(t *testing.T, store *Store, namespaceID string, parent uint64) {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
				NamespaceID: namespaceID, Parent: parent, Name: "contended", Kind: dofs.NodeKindDirectory,
			})
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var successes, rejected int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, dofs.ErrAlreadyExists), errors.Is(err, dofs.ErrConflict):
			rejected++
		default:
			t.Fatalf("concurrent create error = %v", err)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("concurrent create outcomes = %d success, %d rejected", successes, rejected)
	}
}

func TestSQLiteDSNDoesNotInterpretPathAsParameters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata?_pragma=foreign_keys(0).sqlite")
	dsn := sqliteDSN(path, time.Second)
	if want := "foreign_keys%281%29"; !strings.Contains(dsn, want) {
		t.Fatalf("DSN %q does not contain %q", dsn, want)
	}
	store := openTestStore(t, path)
	defer store.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database was not created at literal path: %v", err)
	}
}

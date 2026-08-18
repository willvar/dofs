package postgresintegration_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/willvar/dofs"
	dofspostgres "github.com/willvar/dofs/metadata/postgres"
)

func TestMetadataContract(t *testing.T) {
	dsn := os.Getenv("DOFS_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set DOFS_POSTGRES_TEST_DSN to run the PostgreSQL integration contract")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	store, err := dofspostgres.New(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}

	namespaceID := fmt.Sprintf("test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if _, err := database.Exec(`DELETE FROM dofs_namespaces WHERE id = $1`, namespaceID); err != nil {
			t.Errorf("remove test namespace: %v", err)
		}
	})
	if _, err := store.CreateNamespace(t.Context(), dofs.Namespace{
		ID: namespaceID, Label: "PostgreSQL contract", WrappedKEK: []byte("wrapped-kek"), QuotaBytes: 16,
	}); err != nil {
		t.Fatal(err)
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
	first := createFile(t, store, namespaceID, directory.Inode, "one.txt", "one-v1", 7)
	if _, ok, err := store.CommitGeneration(t.Context(), dofs.CommitGenerationRequest{
		NamespaceID: namespaceID, Inode: first.Inode, ExpectedGeneration: 1,
		ObjectKey: generationKey(t, namespaceID, first.Inode, 2, "too-large"), Size: 17,
	}); !errors.Is(err, dofs.ErrQuotaExceeded) || ok {
		t.Fatalf("quota commit = %v, %v", ok, err)
	}
	second := createFile(t, store, namespaceID, directory.Inode, "two.txt", "two-v1", 3)
	renamed, replaced, err := store.Rename(t.Context(), dofs.RenameRequest{
		NamespaceID: namespaceID, Inode: first.Inode, NewParent: directory.Inode, NewName: second.Name,
	})
	if err != nil || renamed.Name != "two.txt" || replaced == nil || replaced.Inode != second.Inode {
		t.Fatalf("rename replace = %#v, %#v, %v", renamed, replaced, err)
	}
	events, err := store.ListEvents(t.Context(), namespaceID, 0, 100)
	if err != nil || len(events) != 5 || events[3].Kind != dofs.EventRemove || events[4].Kind != dofs.EventRename {
		t.Fatalf("events = %#v, %v", events, err)
	}
	removed, err := store.Remove(t.Context(), dofs.RemoveRequest{
		NamespaceID: namespaceID, Parent: directory.Inode, Name: renamed.Name,
	})
	if err != nil || removed.State != dofs.NodeStateDeleted {
		t.Fatalf("remove = %#v, %v", removed, err)
	}
	for _, inode := range []uint64{second.Inode, first.Inode} {
		if purged, err := store.Purge(t.Context(), namespaceID, inode); err != nil || !purged {
			t.Fatalf("purge %d = %v, %v", inode, purged, err)
		}
	}
	if namespace, err := store.GetNamespace(t.Context(), namespaceID); err != nil || namespace.UsedBytes != 0 {
		t.Fatalf("namespace after purge = %#v, %v", namespace, err)
	}
	testExternalUpload(t, store, namespaceID, directory.Inode)
	testWriterLease(t, store, namespaceID)
	testConcurrentCreate(t, store, namespaceID, directory.Inode)
}

func TestDeleteNamespaceRequiresReleasedWriterLease(t *testing.T) {
	dsn := os.Getenv("DOFS_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set DOFS_POSTGRES_TEST_DSN to run the PostgreSQL integration contract")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store, err := dofspostgres.New(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	namespaceID := fmt.Sprintf("delete-%d", time.Now().UnixNano())
	if _, err := store.CreateNamespace(t.Context(), dofs.Namespace{ID: namespaceID, WrappedKEK: []byte("wrapped")}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireWriterLease(t.Context(), namespaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNamespace(t.Context(), namespaceID); !errors.Is(err, dofs.ErrWriterBusy) {
		t.Fatalf("delete mounted namespace error = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteNamespace(t.Context(), namespaceID); err != nil {
		t.Fatal(err)
	}
}

func testExternalUpload(t *testing.T, store *dofspostgres.Store, namespaceID string, parent uint64) {
	t.Helper()
	upload, err := store.PrepareExternalUpload(t.Context(), dofs.PrepareExternalUploadRequest{
		ID: "postgres-direct", NamespaceID: namespaceID, Parent: parent, Name: "direct.bin",
		Size: 4, WrappedDEK: []byte("direct-wrapped-key"), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Lookup(t.Context(), namespaceID, parent, "direct.bin"); !errors.Is(err, dofs.ErrNotFound) {
		t.Fatalf("reserved node is visible: %v", err)
	}
	refreshedUntil := time.Now().Add(2 * time.Hour)
	refreshed, err := store.RefreshExternalUpload(t.Context(), namespaceID, upload.ID, refreshedUntil)
	if err != nil || refreshed.ExpiresAt.Before(refreshedUntil.Add(-time.Second)) {
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
		ID: "postgres-abort", NamespaceID: namespaceID, Parent: parent, Name: "aborted.bin",
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

func createFile(t *testing.T, store *dofspostgres.Store, namespaceID string, parent uint64, name, transaction string, size int64) dofs.Node {
	t.Helper()
	file, err := store.CreateNode(t.Context(), dofs.CreateNodeRequest{
		NamespaceID: namespaceID, Parent: parent, Name: name, Kind: dofs.NodeKindFile, WrappedDEK: []byte("wrapped-dek"),
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

func testWriterLease(t *testing.T, store *dofspostgres.Store, namespaceID string) {
	t.Helper()
	lease, err := store.AcquireWriterLease(t.Context(), namespaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireWriterLease(t.Context(), namespaceID); !errors.Is(err, dofs.ErrWriterBusy) {
		t.Fatalf("second writer lease = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err = store.AcquireWriterLease(t.Context(), namespaceID)
	if err != nil {
		t.Fatalf("lease after release: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func testConcurrentCreate(t *testing.T, store *dofspostgres.Store, namespaceID string, parent uint64) {
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
	var success, rejected int
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, dofs.ErrAlreadyExists), errors.Is(err, dofs.ErrConflict):
			rejected++
		default:
			t.Fatalf("concurrent create error = %v", err)
		}
	}
	if success != 1 || rejected != 1 {
		t.Fatalf("concurrent outcomes = %d success, %d rejected", success, rejected)
	}
}

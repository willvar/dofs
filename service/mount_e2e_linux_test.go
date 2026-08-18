//go:build linux

package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/willvar/dofs"
	"github.com/willvar/dofs/config"
)

func TestSQLiteS3FUSERestartContract(t *testing.T) {
	if os.Getenv("DOFS_S3_FUSE_INTEGRATION") != "1" {
		t.Skip("set DOFS_S3_FUSE_INTEGRATION=1 to run the SQLite + S3 + real FUSE contract")
	}
	endpoint := environmentOr("DOFS_TEST_S3_ENDPOINT", "127.0.0.1:59000")
	accessKey := environmentOr("DOFS_TEST_S3_ACCESS_KEY", "dofs-test-access")
	secretKey := environmentOr("DOFS_TEST_S3_SECRET_KEY", "dofs-test-secret")
	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "dofs-e2e-" + randomHex(t, 8)
	if err := client.MakeBucket(t.Context(), bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for object := range client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err == nil {
				_ = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			}
		}
		if err := client.RemoveBucket(context.Background(), bucket); err != nil {
			t.Errorf("remove MinIO test bucket: %v", err)
		}
	})

	root := t.TempDir()
	t.Setenv("DOFS_E2E_ACCESS", accessKey)
	t.Setenv("DOFS_E2E_SECRET", secretKey)
	t.Setenv("DOFS_E2E_MASTER", hex.EncodeToString(bytes.Repeat([]byte{0x42}, dofs.KeySize)))
	cfg := config.Default()
	cfg.StateRoot = filepath.Join(root, "state")
	cfg.Metadata.SQLite.Path = filepath.Join(cfg.StateRoot, "metadata.sqlite")
	cfg.Object.S3.Endpoint = endpoint
	cfg.Object.S3.Bucket = bucket
	cfg.Object.S3.Region = "us-east-1"
	cfg.Object.S3.Prefix = "contract"
	cfg.Object.S3.Secure = false
	cfg.Object.S3.AccessKey = config.SecretSource{Env: "DOFS_E2E_ACCESS"}
	cfg.Object.S3.SecretKey = config.SecretSource{Env: "DOFS_E2E_SECRET"}
	cfg.Encryption.MasterKey = config.SecretSource{Env: "DOFS_E2E_MASTER"}
	cfg.Mount.Root = filepath.Join(root, "mounts")
	cfg.Mount.UID = uint32(os.Getuid())
	cfg.Mount.GID = uint32(os.Getgid())

	metadata, err := OpenMetadata(t.Context(), cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := OpenKeyProvider(cfg)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := keys.WrapNewNamespaceKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.CreateNamespace(t.Context(), dofs.Namespace{
		ID: "marvo", Label: "Marvo E2E", WrappedKEK: wrapped, QuotaBytes: 1 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	dofs.Clear(wrapped)
	keys.Close()
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	runtime, err := OpenRuntime(t.Context(), cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertObjectStoreContract(t, runtime.Objects)
	content := []byte("Marvo atomic ordinary-I/O contract through encrypted object storage\n")
	mountpoint := filepath.Join(cfg.Mount.Root, "marvo")
	cancel, mountErrors := startTestMount(t, cfg, runtime, "marvo", mountpoint)
	atomicWrite(t, mountpoint, "config.json", content)
	if got, err := os.ReadFile(filepath.Join(mountpoint, "config.json")); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("mounted read = %q, %v", got, err)
	}
	// The browser/control plane can publish a generation while the FUSE mount
	// owns the one allowed writeback lease. Only ciphertext is sent to the
	// object store; DOFS verifies and atomically exposes it to ordinary I/O.
	control, err := dofs.NewBackend(t.Context(), "marvo", runtime.Metadata, runtime.Objects, runtime.Keys,
		dofs.BackendOptions{ControlPlane: true})
	if err != nil {
		t.Fatal(err)
	}
	directKey := bytes.Repeat([]byte{0x37}, dofs.KeySize)
	directContent := []byte("browser encrypted direct upload while FUSE is mounted\n")
	upload, err := control.PrepareDirectUpload(t.Context(), dofs.DirectUploadRequest{
		ID: "browser-e2e", Parent: dofs.RootInode, Name: "browser.txt",
		Size: int64(len(directContent)), FileKey: directKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	var directCiphertext bytes.Buffer
	if err := dofs.EncryptStream(directKey, bytes.NewReader(directContent), &directCiphertext); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Objects.Put(t.Context(), upload.ObjectKey, bytes.NewReader(directCiphertext.Bytes()), int64(directCiphertext.Len())); err != nil {
		t.Fatal(err)
	}
	if _, err := control.CommitDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}
	if err := control.AcknowledgeDirectUpload(t.Context(), upload.ID); err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(mountpoint, "browser.txt")); err != nil || !bytes.Equal(got, directContent) {
		t.Fatalf("direct upload mounted read = %q, %v", got, err)
	}
	node, err := runtime.Metadata.Lookup(t.Context(), "marvo", dofs.RootInode, "config.json")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := runtime.Objects.GetRange(t.Context(), node.ObjectKey, 0, dofs.EncryptedSize(node.Size, dofs.DefaultChunkSize)-1)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read encrypted object = %v, close = %v", err, closeErr)
	}
	if bytes.Contains(ciphertext, content) || len(ciphertext) == 0 || ciphertext[0] != dofs.CryptoVersion {
		t.Fatal("object store did not contain the expected encrypted DOFS representation")
	}
	cancel()
	if err := <-mountErrors; err != nil {
		t.Fatalf("first unmount: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := OpenRuntime(t.Context(), cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	cancel, mountErrors = startTestMount(t, cfg, restarted, "marvo", mountpoint)
	if got, err := os.ReadFile(filepath.Join(mountpoint, "config.json")); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("read after restart = %q, %v", got, err)
	}
	cancel()
	if err := <-mountErrors; err != nil {
		t.Fatalf("second unmount: %v", err)
	}
	prefix, err := dofs.ObjectRoot("marvo")
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Objects.DeletePrefix(t.Context(), prefix); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertObjectStoreContract(t *testing.T, store dofs.ObjectStore) {
	t.Helper()
	key, err := dofs.GenerationObjectKey("marvo", 999, 1, "immutable-contract")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("encrypted-test-object")
	if err := store.Put(t.Context(), key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if info, err := store.Stat(t.Context(), key); err != nil || info.Size != int64(len(payload)) {
		t.Fatalf("object stat = %#v, %v", info, err)
	}
	if err := store.Put(t.Context(), key, bytes.NewReader([]byte("replacement")), int64(len("replacement"))); !errors.Is(err, dofs.ErrAlreadyExists) {
		t.Fatalf("immutable object overwrite error = %v", err)
	}
	missing, err := store.GetRange(t.Context(), key+"-missing", 0, 0)
	if err == nil {
		_, err = io.ReadAll(missing)
		_ = missing.Close()
	}
	if !errors.Is(err, dofs.ErrObjectNotFound) {
		t.Fatalf("missing object error = %v", err)
	}
	if err := store.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
}

func startTestMount(t *testing.T, cfg config.Config, runtime *Runtime, namespaceID, mountpoint string) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errors := make(chan error, 1)
	go func() {
		errors <- MountNamespace(ctx, cfg, runtime, MountRequest{
			NamespaceID: namespaceID, Mountpoint: mountpoint, Writable: true,
		})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mounted, err := mountedAt(mountpoint)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if mounted {
			return cancel, errors
		}
		select {
		case err := <-errors:
			cancel()
			t.Fatalf("mount exited before becoming ready: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("timed out waiting for real FUSE mount")
		}
	}
}

func atomicWrite(t *testing.T, directory, name string, contents []byte) {
	t.Helper()
	file, err := os.CreateTemp(directory, ".marvo-*")
	if err != nil {
		t.Fatal(err)
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0600); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(contents); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, filepath.Join(directory, name)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, name))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("file mode = %o, want 600", info.Mode().Perm())
	}
}

func randomHex(t *testing.T, size int) string {
	t.Helper()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value)
}

func environmentOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

package s3

import (
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestPhysicalKeyKeepsNamespaceInsideConfiguredPrefix(t *testing.T) {
	client, err := minio.New("127.0.0.1:9000", &minio.Options{Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewWithClient(client, "bucket", "instances/example")
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.physicalKey(".dofs/v1/namespaces/user/objects/1/generation.dofs")
	if err != nil {
		t.Fatal(err)
	}
	want := "instances/example/.dofs/v1/namespaces/user/objects/1/generation.dofs"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	for _, invalid := range []string{"", "/absolute", "../escape", "a/../escape", "a//b"} {
		if _, err := store.physicalKey(invalid); err == nil {
			t.Fatalf("key %q accepted", invalid)
		}
	}
	prefix, err := store.physicalPrefix(".dofs/v1/namespaces/user/objects/1/")
	if err != nil {
		t.Fatal(err)
	}
	if want := "instances/example/.dofs/v1/namespaces/user/objects/1/"; prefix != want {
		t.Fatalf("prefix = %q, want %q", prefix, want)
	}
	if _, err := store.physicalKey(".dofs/v1/namespaces/user/objects/1/"); err == nil {
		t.Fatal("regular object key accepted a trailing slash")
	}
}

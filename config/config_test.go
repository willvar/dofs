package config_test

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willvar/dofs"
	"github.com/willvar/dofs/config"
)

func TestLoadAppliesSafeDefaultsAndSecretSources(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "dofs.yaml")
	master := make([]byte, dofs.KeySize)
	for index := range master {
		master[index] = byte(index + 1)
	}
	t.Setenv("TEST_DOFS_MASTER", "base64:"+base64.StdEncoding.EncodeToString(master))
	t.Setenv("TEST_DOFS_ACCESS", "access")
	t.Setenv("TEST_DOFS_SECRET", "secret")
	contents := fmt.Sprintf(`
version: 1
state_root: %s
metadata:
  driver: sqlite
object:
  driver: s3
  s3:
    endpoint: 127.0.0.1:9000
    bucket: test
    secure: false
    access_key: {env: TEST_DOFS_ACCESS}
    secret_key: {env: TEST_DOFS_SECRET}
encryption:
  master_key: {env: TEST_DOFS_MASTER}
mount:
  root: %s
`, filepath.Join(root, "state"), filepath.Join(root, "mounts"))
	if err := os.WriteFile(configPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Metadata.SQLite.Path != filepath.Join(root, "state", "metadata.sqlite") {
		t.Fatalf("SQLite path = %q", loaded.Metadata.SQLite.Path)
	}
	if !loaded.Mount.Writable || loaded.Metadata.SQLite.BusyTimeoutSeconds != 5 || loaded.Metadata.SQLite.MaxOpenConnections != 8 {
		t.Fatalf("defaults = %#v", loaded)
	}
	key, err := loaded.LoadMasterKey()
	if err != nil {
		t.Fatal(err)
	}
	defer dofs.Clear(key)
	if string(key) != string(master) {
		t.Fatal("decoded master key differs")
	}
}

func TestLoadRejectsInlineSecretsUnknownFieldsAndOverlappingRoots(t *testing.T) {
	root := t.TempDir()
	base := fmt.Sprintf(`
version: 1
state_root: %s
object:
  s3:
    endpoint: 127.0.0.1:9000
    bucket: test
    access_key: {env: ACCESS}
    secret_key: {env: SECRET}
encryption:
  master_key: {env: MASTER}
mount:
  root: %s
`, filepath.Join(root, "state"), filepath.Join(root, "mount"))
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "inline secret", value: strings.Replace(base, "{env: MASTER}", "{value: plaintext}", 1), wantErr: "field value not found"},
		{name: "unknown field", value: base + "unknown: true\n", wantErr: "field unknown not found"},
		{name: "overlap", value: strings.Replace(base, filepath.Join(root, "mount"), filepath.Join(root, "state", "mount"), 1), wantErr: "must not overlap"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(root, strings.ReplaceAll(test.name, " ", "-")+".yaml")
			if err := os.WriteFile(path, []byte(test.value), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := config.Load(path); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestSecretFileIsRelativeToConfig(t *testing.T) {
	root := t.TempDir()
	secretPath := filepath.Join(root, "master.key")
	if err := os.WriteFile(secretPath, []byte(strings.Repeat("01", dofs.KeySize)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "dofs.yaml")
	contents := fmt.Sprintf(`
version: 1
state_root: %s
object:
  s3:
    endpoint: 127.0.0.1:9000
    bucket: test
    access_key: {env: ACCESS}
    secret_key: {env: SECRET}
encryption:
  master_key: {file: master.key}
mount:
  root: %s
`, filepath.Join(root, "state"), filepath.Join(root, "mount"))
	if err := os.WriteFile(configPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Encryption.MasterKey.File != secretPath {
		t.Fatalf("secret file = %q, want %q", loaded.Encryption.MasterKey.File, secretPath)
	}
	key, err := loaded.LoadMasterKey()
	if err != nil {
		t.Fatal(err)
	}
	dofs.Clear(key)
}

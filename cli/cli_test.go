package cli

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willvar/dofs"
)

func TestInitAndNamespaceCommandsUseSQLiteWithoutExposingWrappedKeys(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "dofs.yaml")
	t.Setenv("TEST_DOFS_MASTER", hex.EncodeToString(bytes.Repeat([]byte{7}, dofs.KeySize)))
	contents := fmt.Sprintf(`
version: 1
state_root: %s
object:
  s3:
    endpoint: 127.0.0.1:9000
    bucket: test
    secure: false
    access_key: {env: TEST_ACCESS}
    secret_key: {env: TEST_SECRET}
encryption:
  master_key: {env: TEST_DOFS_MASTER}
mount:
  root: %s
`, filepath.Join(root, "state"), filepath.Join(root, "mounts"))
	if err := os.WriteFile(configPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(arguments ...string) (string, error) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		err := Run(context.Background(), arguments, &stdout, &stderr, Options{})
		if err != nil {
			return stderr.String(), err
		}
		return stdout.String(), nil
	}
	if output, err := run("init", "--config", configPath); err != nil || !strings.Contains(output, "metadata ready (sqlite)") {
		t.Fatalf("init output = %q, error = %v", output, err)
	}
	if output, err := run("namespace", "create", "--config", configPath, "--id", "marvo", "--label", "Marvo", "--quota", "2GiB", "--json"); err != nil {
		t.Fatal(err)
	} else if strings.Contains(output, "wrapped") || !strings.Contains(output, `"quota_bytes": 2147483648`) {
		t.Fatalf("create output = %q", output)
	}
	if output, err := run("namespace", "list", "--config", configPath, "--json"); err != nil {
		t.Fatal(err)
	} else if strings.Contains(output, "wrapped") || !strings.Contains(output, `"id": "marvo"`) {
		t.Fatalf("list output = %q", output)
	}
	if output, err := run("namespace", "show", "--config", configPath, "--id", "marvo"); err != nil || !strings.HasPrefix(output, "marvo\t0\t2147483648\tMarvo") {
		t.Fatalf("show output = %q, error = %v", output, err)
	}
}

func TestParseSize(t *testing.T) {
	for value, want := range map[string]int64{"0": 0, "1KiB": 1024, "2GB": 2_000_000_000, "3 mib": 3 << 20, "unlimited": 0} {
		got, err := parseSize(value)
		if err != nil || got != want {
			t.Fatalf("parseSize(%q) = %d, %v; want %d", value, got, err, want)
		}
	}
	for _, value := range []string{"-1", "1.5GiB", "999999999999999999999TiB"} {
		if _, err := parseSize(value); err == nil {
			t.Fatalf("parseSize(%q) succeeded", value)
		}
	}
}

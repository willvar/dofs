//go:build linux

package service

import (
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNotifySystemdReadyAfterMount(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "notify.sock")
	listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: socketPath, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("NOTIFY_SOCKET", socketPath)
	if err := notifySystemdReady("/mnt/dofs/tenant"); err != nil {
		t.Fatal(err)
	}
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 512)
	count, _, err := listener.ReadFromUnix(buffer)
	if err != nil {
		t.Fatal(err)
	}
	message := string(buffer[:count])
	if !strings.Contains(message, "READY=1") || !strings.Contains(message, "/mnt/dofs/tenant") {
		t.Fatalf("notify message = %q", message)
	}
}

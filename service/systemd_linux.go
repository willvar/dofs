//go:build linux

package service

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// notifySystemdReady is a no-op outside a systemd notify service. Keeping the
// notification at the point where go-fuse has completed its mount handshake
// prevents dependent applications from racing an absent mountpoint.
func notifySystemdReady(mountpoint string) error {
	socket := strings.TrimSpace(os.Getenv("NOTIFY_SOCKET"))
	if socket == "" {
		return nil
	}
	if strings.HasPrefix(socket, "@") {
		socket = "\x00" + strings.TrimPrefix(socket, "@")
	}
	address := &net.UnixAddr{Name: socket, Net: "unixgram"}
	connection, err := net.DialUnix("unixgram", nil, address)
	if err != nil {
		return fmt.Errorf("connect systemd notify socket: %w", err)
	}
	defer connection.Close()
	message := "READY=1\nSTATUS=DOFS mounted at " + mountpoint
	if _, err := connection.Write([]byte(message)); err != nil {
		return fmt.Errorf("notify systemd that DOFS is ready: %w", err)
	}
	return nil
}

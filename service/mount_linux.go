//go:build linux

package service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/willvar/dofs"
	"github.com/willvar/dofs/config"
)

type MountRequest struct {
	NamespaceID string
	Mountpoint  string
	Writable    bool
	Debug       bool
}

func MountNamespace(ctx context.Context, cfg config.Config, runtime *Runtime, request MountRequest) error {
	if runtime == nil || runtime.Metadata == nil || runtime.Objects == nil || runtime.Keys == nil {
		return errors.New("DOFS runtime is incomplete")
	}
	if err := dofs.ValidateNamespaceID(request.NamespaceID); err != nil {
		return err
	}
	if err := dofs.ValidateHostRequirements(cfg.Mount.AllowOther); err != nil {
		return err
	}
	// Do not publish a seemingly healthy FUSE mount when its backing bucket is
	// absent or unreachable. Metadata/key checks happen while constructing the
	// namespace backend below; this closes the remaining startup dependency.
	if err := runtime.checkObjectStore(ctx); err != nil {
		return err
	}
	mountpoint, err := prepareMountpoint(cfg.Mount.Root, request.NamespaceID, request.Mountpoint, cfg.Mount.AllowOther)
	if err != nil {
		return err
	}
	backend, err := dofs.NewBackend(ctx, request.NamespaceID, runtime.Metadata, runtime.Objects, runtime.Keys, dofs.BackendOptions{
		Writable: request.Writable, Mounted: true,
	})
	if err != nil {
		return err
	}
	defer backend.Close()
	if request.Writable {
		stateDirectory := filepath.Join(cfg.StateRoot, "writeback", request.NamespaceID)
		if err := backend.EnableWriteback(ctx, stateDirectory); err != nil {
			return err
		}
	}
	mounted, err := dofs.Mount(mountpoint, backend, dofs.MountOptions{
		Debug: request.Debug, AllowOther: cfg.Mount.AllowOther,
		UID: cfg.Mount.UID, GID: cfg.Mount.GID, Writable: request.Writable,
	})
	if err != nil {
		return err
	}
	if err := notifySystemdReady(mountpoint); err != nil {
		_ = mounted.Unmount()
		return err
	}
	done := make(chan struct{})
	go func() {
		mounted.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		unmountErr := mounted.Unmount()
		<-done
		if errors.Is(context.Cause(ctx), context.Canceled) {
			return unmountErr
		}
		return errors.Join(context.Cause(ctx), unmountErr)
	case <-backend.LeaseLost():
		leaseErr := backend.LeaseError()
		unmountErr := mounted.Unmount()
		<-done
		if leaseErr == nil {
			leaseErr = dofs.ErrLeaseLost
		}
		return errors.Join(leaseErr, unmountErr)
	case <-done:
		return errors.New("DOFS mount stopped unexpectedly")
	}
}

func prepareMountpoint(root, namespaceID, override string, allowOther bool) (string, error) {
	if err := ensureRealDirectory(root, directoryMode(allowOther)); err != nil {
		return "", fmt.Errorf("prepare DOFS mount root: %w", err)
	}
	mountpoint := strings.TrimSpace(override)
	if mountpoint == "" {
		mountpoint = filepath.Join(root, namespaceID)
	}
	if !filepath.IsAbs(mountpoint) {
		return "", errors.New("DOFS mountpoint must be absolute")
	}
	mountpoint = filepath.Clean(mountpoint)
	relative, err := filepath.Rel(root, mountpoint)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("DOFS mountpoint must be inside mount.root and must not equal it")
	}
	mounted, err := mountedAt(mountpoint)
	if err != nil {
		return "", err
	}
	if mounted {
		return "", fmt.Errorf("DOFS mountpoint %s is already mounted", mountpoint)
	}
	if err := ensureRealDirectory(mountpoint, 0700); err != nil {
		return "", fmt.Errorf("prepare DOFS mountpoint: %w", err)
	}
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return "", fmt.Errorf("inspect DOFS mountpoint: %w", err)
	}
	if len(entries) != 0 {
		return "", errors.New("DOFS mountpoint must be empty")
	}
	return mountpoint, nil
}

func directoryMode(allowOther bool) os.FileMode {
	if allowOther {
		return 0711
	}
	return 0700
}

func ensureRealDirectory(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("path must be a real directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if filepath.Clean(resolved) != filepath.Clean(path) {
		return errors.New("path must not contain symbolic-link components")
	}
	return os.Chmod(path, mode)
}

func mountedAt(target string) (bool, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("read Linux mount table: %w", err)
	}
	defer file.Close()
	target = filepath.Clean(target)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && filepath.Clean(decodeMountInfoPath(fields[4])) == target {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan Linux mount table: %w", err)
	}
	return false, nil
}

func decodeMountInfoPath(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
}

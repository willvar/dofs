//go:build linux

package dofs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type fuseNode struct {
	fs.Inode
	backend *Backend
	nodeMu  sync.RWMutex
	node    Node

	writeMu       sync.Mutex
	activeWrite   *WriteSession
	activeWriters int
}

func newFuseRoot(backend *Backend, root Node) *fuseNode {
	return &fuseNode{backend: backend, node: root}
}

func (n *fuseNode) snapshot() Node {
	n.nodeMu.RLock()
	defer n.nodeMu.RUnlock()
	copy := n.node
	copy.WrappedDEK = append([]byte(nil), n.node.WrappedDEK...)
	return copy
}

func (n *fuseNode) setNode(node Node) {
	n.nodeMu.Lock()
	n.node = node
	n.nodeMu.Unlock()
}

// acquireWrite shares one uncommitted plaintext view between every handle for
// an inode. POSIX requires writes through one descriptor to be immediately
// visible through another descriptor, before fsync or close. Keeping separate
// WriteSessions here would make applications that reopen their output while
// writing (for example FFmpeg's faststart pass) read the previous generation.
func (n *fuseNode) acquireWrite(ctx context.Context) (*WriteSession, error) {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.activeWrite == nil {
		session, err := n.backend.OpenWrite(ctx, n.snapshot(), n.setNode)
		if err != nil {
			return nil, err
		}
		n.activeWrite = session
	}
	n.activeWriters++
	return n.activeWrite, nil
}

func (n *fuseNode) releaseWrite(ctx context.Context, session *WriteSession) error {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if session == nil || n.activeWrite != session || n.activeWriters <= 0 {
		return ErrHandleClosed
	}
	n.activeWriters--
	if n.activeWriters > 0 {
		return session.Commit(ctx)
	}
	err := session.Close(ctx)
	n.activeWrite = nil
	return err
}

func (n *fuseNode) readActive(ctx context.Context, destination []byte, offset int64) (int, bool, error) {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.activeWrite == nil {
		return 0, false, nil
	}
	read, err := n.activeWrite.ReadAt(ctx, destination, offset)
	return read, true, err
}

func (n *fuseNode) activeSize() (int64, bool, error) {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.activeWrite == nil {
		return 0, false, nil
	}
	size, err := n.activeWrite.Size()
	return size, true, err
}

func stableAttr(node Node) fs.StableAttr {
	mode := uint32(fuse.S_IFREG)
	if node.IsDir() {
		mode = fuse.S_IFDIR
	}
	return fs.StableAttr{Mode: mode, Ino: node.Inode, Gen: 1}
}

func visibleMode(node Node, writable bool) uint32 {
	mode := node.Mode & 0777
	if writable {
		return mode
	}
	if node.IsDir() {
		return mode & 0555
	}
	return mode & 0444
}

func fillAttr(out *fuse.Attr, node Node, writable bool) {
	if node.IsDir() {
		out.Mode = fuse.S_IFDIR | visibleMode(node, writable)
		out.Nlink = 2
	} else {
		out.Mode = fuse.S_IFREG | visibleMode(node, writable)
		out.Nlink = 1
		out.Size = uint64(node.Size)
	}
	out.SetTimes(&node.CreatedAt, &node.ModifiedAt, &node.ChangedAt)
}

func (n *fuseNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if file, ok := handle.(*fileHandle); ok {
		return file.Getattr(ctx, out)
	}
	node := n.snapshot()
	fillAttr(&out.Attr, node, n.backend.Writable())
	if node.IsFile() {
		if size, active, err := n.activeSize(); err != nil {
			return errno(err)
		} else if active {
			out.Size = uint64(size)
		}
	}
	return 0
}

func (n *fuseNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	parent := n.snapshot()
	if !parent.IsDir() {
		return nil, syscall.ENOTDIR
	}
	node, err := n.backend.Lookup(ctx, parent.Inode, name)
	if err != nil {
		return nil, errno(err)
	}
	if existing := n.GetChild(name); existing != nil {
		if operations, ok := existing.Operations().(*fuseNode); ok {
			operations.setNode(node)
			fillAttr(&out.Attr, node, n.backend.Writable())
		}
		return existing, 0
	}
	childOperations := &fuseNode{backend: n.backend, node: node}
	child := n.NewInode(ctx, childOperations, stableAttr(node))
	fillAttr(&out.Attr, node, n.backend.Writable())
	return child, 0
}

func (n *fuseNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	parent := n.snapshot()
	if !parent.IsDir() {
		return nil, syscall.ENOTDIR
	}
	nodes, err := n.backend.List(ctx, parent.Inode)
	if err != nil {
		return nil, errno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(nodes))
	for _, node := range nodes {
		attribute := stableAttr(node)
		entries = append(entries, fuse.DirEntry{Name: node.Name, Mode: attribute.Mode, Ino: attribute.Ino})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *fuseNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	parent := n.snapshot()
	if !parent.IsDir() {
		return nil, syscall.ENOTDIR
	}
	node, err := n.backend.CreateDirectory(ctx, parent.Inode, name, mode)
	if err != nil {
		return nil, errno(err)
	}
	childOperations := &fuseNode{backend: n.backend, node: node}
	child := n.NewInode(ctx, childOperations, stableAttr(node))
	fillAttr(&out.Attr, node, true)
	return child, 0
}

func (n *fuseNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	parent := n.snapshot()
	if !parent.IsDir() {
		return nil, nil, 0, syscall.ENOTDIR
	}
	node, err := n.backend.CreateFile(ctx, parent.Inode, name, mode)
	if err != nil {
		return nil, nil, 0, errno(err)
	}
	childOperations := &fuseNode{backend: n.backend, node: node}
	child := n.NewInode(ctx, childOperations, stableAttr(node))
	if err := n.backend.retain(node.Inode); err != nil {
		_ = n.backend.Remove(context.Background(), parent.Inode, name, false)
		return nil, nil, 0, errno(err)
	}
	handle := &fileHandle{
		backend: n.backend, node: childOperations, metadata: node,
		writable: flags&syscall.O_ACCMODE != syscall.O_RDONLY, retained: true,
	}
	fillAttr(&out.Attr, node, true)
	return child, handle, 0, 0
}

func (n *fuseNode) Unlink(ctx context.Context, name string) syscall.Errno {
	parent := n.snapshot()
	if !parent.IsDir() {
		return syscall.ENOTDIR
	}
	return errno(n.backend.Remove(ctx, parent.Inode, name, false))
}

func (n *fuseNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	parent := n.snapshot()
	if !parent.IsDir() {
		return syscall.ENOTDIR
	}
	return errno(n.backend.Remove(ctx, parent.Inode, name, true))
}

func (n *fuseNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags & ^uint32(unix.RENAME_NOREPLACE) != 0 {
		return syscall.ENOTSUP
	}
	parent := n.snapshot()
	if !parent.IsDir() {
		return syscall.ENOTDIR
	}
	newParentNode, ok := newParent.(*fuseNode)
	if !ok || newParentNode.backend != n.backend || !newParentNode.snapshot().IsDir() {
		return syscall.EXDEV
	}
	source, err := n.backend.Lookup(ctx, parent.Inode, name)
	if err != nil {
		return errno(err)
	}
	renamed, err := n.backend.Rename(
		ctx, source.Inode, newParentNode.snapshot().Inode, newName,
		flags&uint32(unix.RENAME_NOREPLACE) != 0,
	)
	if err != nil {
		return errno(err)
	}
	if child := n.GetChild(name); child != nil {
		if operations, ok := child.Operations().(*fuseNode); ok {
			operations.setNode(renamed)
		}
	}
	return 0
}

func (n *fuseNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	node := n.snapshot()
	if node.IsDir() {
		return nil, 0, syscall.EISDIR
	}
	wantsWrite := flags&syscall.O_ACCMODE != syscall.O_RDONLY
	if wantsWrite && !n.backend.WritebackEnabled() {
		return nil, 0, syscall.EROFS
	}
	if err := n.backend.retain(node.Inode); err != nil {
		return nil, 0, errno(err)
	}
	handle := &fileHandle{backend: n.backend, node: n, metadata: node, writable: wantsWrite, retained: true}
	if wantsWrite && flags&syscall.O_TRUNC != 0 {
		writeSession, err := handle.ensureWrite(ctx)
		if err != nil {
			_ = n.backend.release(context.Background(), node.Inode)
			return nil, 0, errno(err)
		}
		if err := writeSession.Truncate(ctx, 0); err != nil {
			if handle.sharedWrite {
				_ = n.releaseWrite(ctx, writeSession)
			} else {
				writeSession.Abort()
			}
			_ = n.backend.release(context.Background(), node.Inode)
			return nil, 0, errno(err)
		}
	}
	return handle, 0, 0
}

func (n *fuseNode) Setattr(ctx context.Context, handle fs.FileHandle, input *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	allowed := uint32(fuse.FATTR_MODE | fuse.FATTR_SIZE | fuse.FATTR_FH | fuse.FATTR_LOCKOWNER | fuse.FATTR_KILL_SUIDGID)
	if input.Valid & ^allowed != 0 {
		return syscall.EPERM
	}
	if file, ok := handle.(*fileHandle); ok {
		return file.Setattr(ctx, input, out)
	}
	node := n.snapshot()
	if mode, ok := input.GetMode(); ok {
		updated, err := n.backend.Chmod(ctx, node.Inode, mode)
		if err != nil {
			return errno(err)
		}
		n.setNode(updated)
		node = updated
	}
	if size, ok := input.GetSize(); ok {
		if node.IsDir() || !n.backend.WritebackEnabled() {
			return syscall.EROFS
		}
		if err := n.backend.retain(node.Inode); err != nil {
			return errno(err)
		}
		defer func() { _ = n.backend.release(context.Background(), node.Inode) }()
		session, err := n.acquireWrite(ctx)
		if err != nil {
			return errno(err)
		}
		if err := session.Truncate(ctx, int64(size)); err != nil {
			_ = n.releaseWrite(ctx, session)
			return errno(err)
		}
		if err := n.releaseWrite(ctx, session); err != nil {
			return errno(err)
		}
		node = n.snapshot()
	}
	fillAttr(&out.Attr, node, n.backend.Writable())
	return 0
}

type fileHandle struct {
	backend     *Backend
	node        *fuseNode
	metadata    Node
	writable    bool
	writeMu     sync.Mutex
	write       *WriteSession
	sharedWrite bool
	retained    bool
	release     sync.Once
	releaseErr  syscall.Errno
}

func (h *fileHandle) ensureWrite(ctx context.Context) (*WriteSession, error) {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if h.write != nil {
		return h.write, nil
	}
	if !h.writable {
		return nil, ErrReadOnly
	}
	if h.node != nil {
		writeSession, err := h.node.acquireWrite(ctx)
		if err != nil {
			return nil, err
		}
		h.metadata = h.node.snapshot()
		h.write = writeSession
		h.sharedWrite = true
		return writeSession, nil
	}
	writeSession, err := h.backend.OpenWrite(ctx, h.metadata, nil)
	if err != nil {
		return nil, err
	}
	h.write = writeSession
	return writeSession, nil
}

func (h *fileHandle) writeSession() *WriteSession {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return h.write
}

func (h *fileHandle) Read(ctx context.Context, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	writeSession := h.writeSession()
	if h.writable && writeSession == nil {
		var err error
		writeSession, err = h.ensureWrite(ctx)
		if err != nil {
			return nil, errno(err)
		}
	}
	if writeSession != nil {
		read, err := writeSession.ReadAt(ctx, destination, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, errno(err)
		}
		return fuse.ReadResultData(destination[:read]), 0
	}
	if h.node != nil {
		read, active, err := h.node.readActive(ctx, destination, offset)
		if active {
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, errno(err)
			}
			return fuse.ReadResultData(destination[:read]), 0
		}
	}
	metadata := h.metadata
	if h.node != nil {
		metadata = h.node.snapshot()
	}
	data, err := h.backend.ReadAt(ctx, metadata, offset, len(destination))
	if err != nil {
		return nil, errno(err)
	}
	return fuse.ReadResultData(data), 0
}

func (h *fileHandle) Write(ctx context.Context, data []byte, offset int64) (uint32, syscall.Errno) {
	writeSession, err := h.ensureWrite(ctx)
	if err != nil {
		return 0, errno(err)
	}
	written, err := writeSession.WriteAt(ctx, data, offset)
	return uint32(written), errno(err)
}

func (h *fileHandle) Flush(ctx context.Context) syscall.Errno {
	writeSession := h.writeSession()
	if writeSession == nil {
		return 0
	}
	return errno(writeSession.Commit(ctx))
}

func (h *fileHandle) Fsync(ctx context.Context, _ uint32) syscall.Errno { return h.Flush(ctx) }

func (h *fileHandle) Release(ctx context.Context) syscall.Errno {
	h.release.Do(func() {
		if writeSession := h.writeSession(); writeSession != nil {
			if h.sharedWrite && h.node != nil {
				h.releaseErr = errno(h.node.releaseWrite(ctx, writeSession))
			} else {
				h.releaseErr = errno(writeSession.Close(ctx))
			}
		}
		if h.retained {
			if releaseErr := errno(h.backend.release(context.Background(), h.metadata.Inode)); h.releaseErr == 0 {
				h.releaseErr = releaseErr
			}
			h.retained = false
		}
	})
	return h.releaseErr
}

func (h *fileHandle) Getattr(_ context.Context, out *fuse.AttrOut) syscall.Errno {
	node := h.metadata
	if h.node != nil {
		node = h.node.snapshot()
	}
	fillAttr(&out.Attr, node, h.backend.Writable())
	if writeSession := h.writeSession(); writeSession != nil {
		size, err := writeSession.Size()
		if err != nil {
			return errno(err)
		}
		out.Size = uint64(size)
	} else if h.node != nil {
		if size, active, err := h.node.activeSize(); err != nil {
			return errno(err)
		} else if active {
			out.Size = uint64(size)
		}
	}
	return 0
}

func (h *fileHandle) Setattr(ctx context.Context, input *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	allowed := uint32(fuse.FATTR_MODE | fuse.FATTR_SIZE | fuse.FATTR_FH | fuse.FATTR_LOCKOWNER | fuse.FATTR_KILL_SUIDGID)
	if input.Valid & ^allowed != 0 {
		return syscall.EPERM
	}
	if mode, ok := input.GetMode(); ok {
		updated, err := h.backend.Chmod(ctx, h.metadata.Inode, mode)
		if err != nil {
			return errno(err)
		}
		h.metadata = updated
		if h.node != nil {
			h.node.setNode(updated)
		}
	}
	if size, ok := input.GetSize(); ok {
		writeSession, err := h.ensureWrite(ctx)
		if err != nil {
			return errno(err)
		}
		if err := writeSession.Truncate(ctx, int64(size)); err != nil {
			return errno(err)
		}
	}
	return h.Getattr(ctx, out)
}

func errno(err error) syscall.Errno {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrObjectNotFound):
		return syscall.ENOENT
	case errors.Is(err, ErrInvalidName), errors.Is(err, ErrInvalidNamespace):
		return syscall.EINVAL
	case errors.Is(err, ErrAlreadyExists):
		return syscall.EEXIST
	case errors.Is(err, ErrDirectoryNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, ErrNotDirectory):
		return syscall.ENOTDIR
	case errors.Is(err, ErrTypeMismatch):
		return syscall.EINVAL
	case errors.Is(err, ErrIsDirectory):
		return syscall.EISDIR
	case errors.Is(err, ErrReadOnly):
		return syscall.EROFS
	case errors.Is(err, ErrWriteConflict), errors.Is(err, ErrConflict):
		return syscall.ESTALE
	case errors.Is(err, ErrQuotaExceeded):
		return syscall.ENOSPC
	case errors.Is(err, ErrHandleClosed):
		return syscall.EBADF
	default:
		return syscall.EIO
	}
}

// Mount exposes a backend at mountpoint and returns after the kernel mount is
// ready. The caller owns Unmount/Wait and the backend lifecycle.
func Mount(mountpoint string, backend *Backend, options MountOptions) (MountedFilesystem, error) {
	if backend == nil {
		return nil, errors.New("DOFS backend is required")
	}
	if options.Writable && !backend.WritebackEnabled() {
		return nil, errors.New("DOFS writeback is not enabled")
	}
	root, err := backend.GetNode(context.Background(), RootInode)
	if err != nil {
		return nil, fmt.Errorf("load DOFS mount root: %w", err)
	}
	oneSecond := time.Second
	mountOptions := []string{"default_permissions", "nosuid", "nodev"}
	if !options.Writable {
		mountOptions = append(mountOptions, "ro")
	}
	fsOptions := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: options.AllowOther, Options: mountOptions,
			FsName: "dofs:" + backend.namespace.ID, Name: "dofs", Debug: options.Debug,
			DisableXAttrs: true, MaxWrite: 128 * 1024, MaxReadAhead: 128 * 1024,
		},
		EntryTimeout: &oneSecond, AttrTimeout: &oneSecond, NegativeTimeout: &oneSecond,
		UID: options.UID, GID: options.GID,
		RootStableAttr: &fs.StableAttr{Mode: fuse.S_IFDIR, Ino: RootInode, Gen: 1},
	}
	return fs.Mount(mountpoint, newFuseRoot(backend, root), fsOptions)
}

var (
	_ fs.NodeGetattrer = (*fuseNode)(nil)
	_ fs.NodeSetattrer = (*fuseNode)(nil)
	_ fs.NodeLookuper  = (*fuseNode)(nil)
	_ fs.NodeReaddirer = (*fuseNode)(nil)
	_ fs.NodeOpener    = (*fuseNode)(nil)
	_ fs.NodeMkdirer   = (*fuseNode)(nil)
	_ fs.NodeCreater   = (*fuseNode)(nil)
	_ fs.NodeUnlinker  = (*fuseNode)(nil)
	_ fs.NodeRmdirer   = (*fuseNode)(nil)
	_ fs.NodeRenamer   = (*fuseNode)(nil)
	_ fs.FileReader    = (*fileHandle)(nil)
	_ fs.FileWriter    = (*fileHandle)(nil)
	_ fs.FileFlusher   = (*fileHandle)(nil)
	_ fs.FileFsyncer   = (*fileHandle)(nil)
	_ fs.FileReleaser  = (*fileHandle)(nil)
	_ fs.FileGetattrer = (*fileHandle)(nil)
	_ fs.FileSetattrer = (*fileHandle)(nil)
)

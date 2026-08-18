package dofs

type MountOptions struct {
	Debug      bool
	AllowOther bool
	UID        uint32
	GID        uint32
	Writable   bool
}

type MountedFilesystem interface {
	Unmount() error
	Wait()
}

func (b *Backend) WritebackEnabled() bool {
	if b == nil {
		return false
	}
	b.writebackMu.Lock()
	defer b.writebackMu.Unlock()
	return b.writeback != nil
}

package dofs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
)

// ListGenerations returns the immutable versions owned by one file inode in
// ascending generation order. The current generation is included in the
// result; callers can identify it by comparing Number with Node.Generation.
func (b *Backend) ListGenerations(ctx context.Context, inode uint64) ([]Generation, error) {
	if b == nil {
		return nil, errors.New("dofs: backend is required")
	}
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err != nil {
		return nil, err
	}
	if !b.validNode(node, true) || !node.IsFile() {
		return nil, ErrNotFound
	}
	generations, err := b.metadata.ListGenerations(ctx, b.namespace.ID, inode)
	if err != nil {
		return nil, err
	}
	for _, generation := range generations {
		if generation.NamespaceID != b.namespace.ID || generation.Inode != inode || generation.Number <= 0 || generation.Size < 0 {
			return nil, fmt.Errorf("dofs: corrupt generation metadata for inode %d", inode)
		}
		if err := ValidateGenerationObjectKey(b.namespace.ID, inode, generation.ObjectKey); err != nil {
			return nil, err
		}
	}
	return generations, nil
}

// GetGeneration returns one immutable version, including the current version.
func (b *Backend) GetGeneration(ctx context.Context, inode uint64, number int64) (Generation, error) {
	if number <= 0 {
		return Generation{}, ErrNotFound
	}
	generations, err := b.ListGenerations(ctx, inode)
	if err != nil {
		return Generation{}, err
	}
	for _, generation := range generations {
		if generation.Number == number {
			return generation, nil
		}
	}
	return Generation{}, ErrNotFound
}

// OpenGenerationPlaintext opens a historical generation without changing the
// current inode. The generation carries its own wrapped DEK because a direct
// replacement may have rotated the file key.
func (b *Backend) OpenGenerationPlaintext(ctx context.Context, inode uint64, number int64) (io.ReadCloser, error) {
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err != nil {
		return nil, err
	}
	if !b.validNode(node, true) || !node.IsFile() {
		return nil, ErrNotFound
	}
	generation, err := b.GetGeneration(ctx, inode, number)
	if err != nil {
		return nil, err
	}
	if len(generation.WrappedDEK) == 0 && generation.Number == node.Generation {
		generation.WrappedDEK = append([]byte(nil), node.WrappedDEK...)
	}
	node.Generation = generation.Number
	node.ObjectKey = generation.ObjectKey
	node.Size = generation.Size
	node.WrappedDEK = append([]byte(nil), generation.WrappedDEK...)
	if len(node.WrappedDEK) == 0 {
		return nil, errors.New("dofs: historical generation has no wrapped data key")
	}
	return b.OpenPlaintext(ctx, node)
}

// RestoreGeneration publishes a copy of an immutable historical object as a
// new current generation. The old current generation remains in the history,
// and a concurrent write wins the CAS rather than being overwritten.
func (b *Backend) RestoreGeneration(ctx context.Context, inode uint64, number, expectedCurrentGeneration int64) (Node, error) {
	if err := b.requireMutation(); err != nil {
		return Node{}, err
	}
	node, err := b.metadata.GetNode(ctx, b.namespace.ID, inode)
	if err != nil {
		return Node{}, err
	}
	if !b.validNode(node, false) || !node.IsFile() {
		return Node{}, ErrNotFound
	}
	if expectedCurrentGeneration > 0 && node.Generation != expectedCurrentGeneration {
		return node, ErrConflict
	}
	source, err := b.GetGeneration(ctx, inode, number)
	if err != nil {
		return Node{}, err
	}
	if len(source.WrappedDEK) == 0 && source.Number == node.Generation {
		source.WrappedDEK = append([]byte(nil), node.WrappedDEK...)
	}
	if len(source.WrappedDEK) == 0 {
		return Node{}, errors.New("dofs: historical generation has no wrapped data key")
	}
	if node.Generation == math.MaxInt64 {
		return Node{}, errors.New("dofs: generation space exhausted")
	}
	transactionID, err := randomTransactionID()
	if err != nil {
		return Node{}, err
	}
	targetKey, err := GenerationObjectKey(b.namespace.ID, inode, node.Generation+1, transactionID)
	if err != nil {
		return Node{}, err
	}
	if err := b.objects.Copy(ctx, source.ObjectKey, targetKey); err != nil {
		return Node{}, fmt.Errorf("copy historical generation: %w", err)
	}
	committed, ok, err := b.metadata.CommitGeneration(ctx, CommitGenerationRequest{
		NamespaceID: b.namespace.ID, Inode: inode, ExpectedGeneration: node.Generation,
		ObjectKey: targetKey, Size: source.Size, WrappedDEK: source.WrappedDEK,
	})
	if err != nil || !ok {
		_ = b.objects.Delete(context.Background(), targetKey)
		if err != nil {
			return committed, err
		}
		return committed, ErrConflict
	}
	return committed, nil
}

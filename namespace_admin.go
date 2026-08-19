package dofs

import (
	"context"
	"errors"
	"fmt"
)

// DeleteNamespace permanently removes a namespace catalog before reclaiming
// its exact encrypted-object prefix. It is intentionally idempotent after the
// metadata step so operators can retry an interrupted object cleanup without
// restoring the now-unreachable namespace.
func DeleteNamespace(ctx context.Context, metadata MetadataStore, objects ObjectStore, namespaceID string) error {
	if metadata == nil || objects == nil {
		return errors.New("dofs: metadata and object stores are required")
	}
	if err := ValidateNamespaceID(namespaceID); err != nil {
		return err
	}
	if _, err := metadata.GetNamespace(ctx, namespaceID); errors.Is(err, ErrNotFound) {
		prefix, prefixErr := ObjectRoot(namespaceID)
		if prefixErr != nil {
			return prefixErr
		}
		if deleteErr := objects.DeletePrefix(ctx, prefix); deleteErr != nil && !errors.Is(deleteErr, ErrObjectNotFound) {
			return fmt.Errorf("delete DOFS namespace objects: %w", deleteErr)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect DOFS namespace: %w", err)
	}
	if err := metadata.DeleteNamespace(ctx, namespaceID); err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("delete DOFS namespace metadata: %w", err)
	}
	prefix, err := ObjectRoot(namespaceID)
	if err != nil {
		return err
	}
	if err := objects.DeletePrefix(ctx, prefix); err != nil && !errors.Is(err, ErrObjectNotFound) {
		return fmt.Errorf("delete DOFS namespace objects: %w", err)
	}
	return nil
}

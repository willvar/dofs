package dofs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultSupersededGenerationGrace deliberately does not protect already
// issued browser/object-store read URLs. Once a generation falls outside the
// namespace history limit, the next reclaim pass may delete it. Callers that
// explicitly need read-URL protection can pass a non-zero grace to Reclaim.
const DefaultSupersededGenerationGrace time.Duration = 0

type ReclaimOptions struct {
	SupersededGenerationGrace time.Duration
}

// ReclaimResult describes one idempotent namespace reclamation pass. A
// deferred pass is not an error: its tombstones remain the durable retry queue.
type ReclaimResult struct {
	NamespaceID         string
	Scanned             int
	Purged              int
	Deferred            int
	PrunedGenerations   int
	DeferredGenerations int
	Busy                bool
}

// ReclaimDeleted physically removes every immutable generation owned by
// deleted inodes, then purges their tombstone metadata. Deleted nodes are the
// durable queue: a crash or object-store failure can safely retry this method.
//
// A namespace-wide exclusive reclamation lease prevents deletion while any
// FUSE mount may still hold an open Unix handle. Active direct-upload URLs are
// also allowed to expire before their inode is purged so a late client cannot
// recreate an untracked object after reclamation.
func ReclaimDeleted(
	ctx context.Context,
	metadata MetadataStore,
	objects ObjectStore,
	namespaceID string,
) (ReclaimResult, error) {
	return Reclaim(ctx, metadata, objects, namespaceID, ReclaimOptions{
		SupersededGenerationGrace: DefaultSupersededGenerationGrace,
	})
}

// Reclaim is ReclaimDeleted with an optional explicit superseded-generation
// grace. The normal application policy is zero: over-limit generations are
// eligible for deletion on this pass.
func Reclaim(
	ctx context.Context,
	metadata MetadataStore,
	objects ObjectStore,
	namespaceID string,
	options ReclaimOptions,
) (ReclaimResult, error) {
	result := ReclaimResult{NamespaceID: namespaceID}
	if metadata == nil || objects == nil {
		return result, errors.New("dofs: metadata and object stores are required")
	}
	if err := ValidateNamespaceID(namespaceID); err != nil {
		return result, err
	}
	if options.SupersededGenerationGrace < 0 {
		return result, errors.New("dofs: superseded generation grace cannot be negative")
	}
	lease, err := metadata.AcquireReclaimLease(ctx, namespaceID)
	if errors.Is(err, ErrReclaimBusy) {
		result.Busy = true
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("acquire DOFS reclamation lease: %w", err)
	}
	defer lease.Close()
	namespace, err := metadata.GetNamespace(ctx, namespaceID)
	if err != nil {
		return result, fmt.Errorf("load DOFS namespace history policy: %w", err)
	}
	historyLimit := namespace.HistoryLimit
	if historyLimit < DefaultHistoryLimit {
		historyLimit = DefaultHistoryLimit
	}

	deleted, err := metadata.ListState(ctx, namespaceID, NodeStateDeleted)
	if err != nil {
		return result, fmt.Errorf("list deleted DOFS inodes: %w", err)
	}
	activeUploads, err := metadata.ListExternalUploads(ctx, namespaceID, ExternalUploadActive)
	if err != nil {
		return result, fmt.Errorf("list active DOFS uploads: %w", err)
	}
	committedUploads, err := metadata.ListExternalUploads(ctx, namespaceID, ExternalUploadCommitted)
	if err != nil {
		return result, fmt.Errorf("list committed DOFS uploads: %w", err)
	}
	uploadsByInode := make(map[uint64][]ExternalUpload)
	for _, upload := range append(activeUploads, committedUploads...) {
		uploadsByInode[upload.Inode] = append(uploadsByInode[upload.Inode], upload)
	}

	now := time.Now().UTC()
	var reclaimErrors []error
	for _, node := range deleted {
		if leaseErr := requireLiveLease(lease); leaseErr != nil {
			return result, errors.Join(append(reclaimErrors, leaseErr)...)
		}
		result.Scanned++
		uploads := uploadsByInode[node.Inode]
		deferForUpload := false
		for _, upload := range uploads {
			if upload.State == ExternalUploadActive && now.Before(upload.ExpiresAt) {
				deferForUpload = true
				break
			}
		}
		if deferForUpload {
			result.Deferred++
			continue
		}
		generations, listErr := metadata.ListGenerations(ctx, namespaceID, node.Inode)
		if listErr != nil {
			reclaimErrors = append(reclaimErrors, fmt.Errorf("list inode %d generations: %w", node.Inode, listErr))
			continue
		}
		keys := make(map[string]struct{}, len(generations)+len(uploads))
		for _, generation := range generations {
			keys[generation.ObjectKey] = struct{}{}
		}
		for _, upload := range uploads {
			keys[upload.ObjectKey] = struct{}{}
		}
		failed := false
		for key := range keys {
			if leaseErr := requireLiveLease(lease); leaseErr != nil {
				return result, errors.Join(append(reclaimErrors, leaseErr)...)
			}
			if deleteErr := objects.Delete(ctx, key); deleteErr != nil && !errors.Is(deleteErr, ErrObjectNotFound) {
				reclaimErrors = append(reclaimErrors, fmt.Errorf("delete inode %d object %q: %w", node.Inode, key, deleteErr))
				failed = true
			}
		}
		if failed {
			continue
		}
		if leaseErr := requireLiveLease(lease); leaseErr != nil {
			return result, errors.Join(append(reclaimErrors, leaseErr)...)
		}
		purged, purgeErr := metadata.Purge(ctx, namespaceID, node.Inode, node.Generation)
		if errors.Is(purgeErr, ErrConflict) {
			result.Deferred++
			continue
		}
		if purgeErr != nil {
			reclaimErrors = append(reclaimErrors, fmt.Errorf("purge inode %d metadata: %w", node.Inode, purgeErr))
			continue
		}
		if purged {
			result.Purged++
		}
	}
	ready, err := metadata.ListState(ctx, namespaceID, NodeStateReady)
	if err != nil {
		reclaimErrors = append(reclaimErrors, fmt.Errorf("list ready DOFS inodes: %w", err))
		return result, errors.Join(reclaimErrors...)
	}
	for _, node := range ready {
		if leaseErr := requireLiveLease(lease); leaseErr != nil {
			return result, errors.Join(append(reclaimErrors, leaseErr)...)
		}
		if !node.IsFile() || node.Generation <= 1 {
			continue
		}
		generations, listErr := metadata.ListGenerations(ctx, namespaceID, node.Inode)
		if listErr != nil {
			reclaimErrors = append(reclaimErrors, fmt.Errorf("list inode %d generations: %w", node.Inode, listErr))
			continue
		}
		var currentCreatedAt time.Time
		for _, generation := range generations {
			if generation.Number == node.Generation {
				currentCreatedAt = generation.CreatedAt
				break
			}
		}
		if currentCreatedAt.IsZero() {
			reclaimErrors = append(reclaimErrors, fmt.Errorf(
				"inode %d current generation %d is missing", node.Inode, node.Generation,
			))
			continue
		}
		retained := make(map[int64]struct{})
		for index := len(generations) - 1; index >= 0 && int64(len(retained)) < historyLimit; index-- {
			retained[generations[index].Number] = struct{}{}
		}
		if options.SupersededGenerationGrace > 0 && now.Before(currentCreatedAt.Add(options.SupersededGenerationGrace)) {
			for _, generation := range generations {
				if generation.Number < node.Generation {
					if _, keep := retained[generation.Number]; keep {
						continue
					}
					result.DeferredGenerations++
				}
			}
			continue
		}
		for _, generation := range generations {
			if generation.Number >= node.Generation {
				continue
			}
			if _, keep := retained[generation.Number]; keep {
				continue
			}
			if leaseErr := requireLiveLease(lease); leaseErr != nil {
				return result, errors.Join(append(reclaimErrors, leaseErr)...)
			}
			if deleteErr := objects.Delete(ctx, generation.ObjectKey); deleteErr != nil && !errors.Is(deleteErr, ErrObjectNotFound) {
				reclaimErrors = append(reclaimErrors, fmt.Errorf(
					"delete inode %d superseded generation %d: %w", node.Inode, generation.Number, deleteErr,
				))
				continue
			}
			if leaseErr := requireLiveLease(lease); leaseErr != nil {
				return result, errors.Join(append(reclaimErrors, leaseErr)...)
			}
			deleted, deleteErr := metadata.DeleteGeneration(ctx, namespaceID, node.Inode, generation.Number)
			if deleteErr != nil {
				reclaimErrors = append(reclaimErrors, fmt.Errorf(
					"forget inode %d superseded generation %d: %w", node.Inode, generation.Number, deleteErr,
				))
				continue
			}
			if deleted {
				result.PrunedGenerations++
			}
		}
	}
	return result, errors.Join(reclaimErrors...)
}

func requireLiveLease(lease Lease) error {
	if lease == nil || lease.Lost() == nil {
		return nil
	}
	select {
	case <-lease.Lost():
		if err := lease.Err(); err != nil {
			return fmt.Errorf("%w: %v", ErrLeaseLost, err)
		}
		return ErrLeaseLost
	default:
		return nil
	}
}

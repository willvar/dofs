// Package service wires application-neutral DOFS interfaces to configured
// production adapters. It contains no Domus or workload-specific policy.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/willvar/dofs"
	"github.com/willvar/dofs/config"
	"github.com/willvar/dofs/metadata/sqlite"
	s3store "github.com/willvar/dofs/object/s3"
)

type Metadata interface {
	dofs.MetadataStore
	Migrate(context.Context) error
	io.Closer
}

type PostgresOpener func(context.Context, string) (Metadata, error)

type Options struct {
	OpenPostgres PostgresOpener
}

func OpenMetadata(ctx context.Context, cfg config.Config, options Options) (Metadata, error) {
	switch cfg.Metadata.Driver {
	case "sqlite":
		store, err := sqlite.Open(ctx, sqlite.Config{
			Path:               cfg.Metadata.SQLite.Path,
			BusyTimeout:        time.Duration(cfg.Metadata.SQLite.BusyTimeoutSeconds) * time.Second,
			MaxOpenConnections: cfg.Metadata.SQLite.MaxOpenConnections,
		})
		if err != nil {
			return nil, err
		}
		if err := store.Migrate(ctx); err != nil {
			_ = store.Close()
			return nil, err
		}
		return store, nil
	case "postgres":
		if options.OpenPostgres == nil {
			return nil, errors.New("PostgreSQL metadata support is not included in this dofsd build")
		}
		dsn, err := cfg.Metadata.Postgres.DSN.Read(true)
		if err != nil {
			return nil, fmt.Errorf("load PostgreSQL DSN: %w", err)
		}
		store, err := options.OpenPostgres(ctx, dsn)
		if err != nil {
			return nil, err
		}
		if err := store.Migrate(ctx); err != nil {
			_ = store.Close()
			return nil, err
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unsupported metadata driver %q", cfg.Metadata.Driver)
	}
}

func OpenObjectStore(cfg config.Config) (dofs.ObjectStore, error) {
	if cfg.Object.Driver != "s3" {
		return nil, fmt.Errorf("unsupported object driver %q", cfg.Object.Driver)
	}
	accessKey, err := cfg.Object.S3.AccessKey.Read(true)
	if err != nil {
		return nil, fmt.Errorf("load S3 access key: %w", err)
	}
	secretKey, err := cfg.Object.S3.SecretKey.Read(true)
	if err != nil {
		return nil, fmt.Errorf("load S3 secret key: %w", err)
	}
	sessionToken, err := cfg.Object.S3.SessionToken.Read(false)
	if err != nil {
		return nil, fmt.Errorf("load S3 session token: %w", err)
	}
	return s3store.New(s3store.Config{
		Endpoint: cfg.Object.S3.Endpoint, AccessKey: accessKey, SecretKey: secretKey,
		SessionToken: sessionToken, Bucket: cfg.Object.S3.Bucket, Region: cfg.Object.S3.Region,
		Prefix: cfg.Object.S3.Prefix, Secure: cfg.Object.S3.Secure,
	})
}

func OpenKeyProvider(cfg config.Config) (*dofs.EnvelopeKeyProvider, error) {
	masterKey, err := cfg.LoadMasterKey()
	if err != nil {
		return nil, err
	}
	defer dofs.Clear(masterKey)
	return dofs.NewEnvelopeKeyProvider(masterKey)
}

type Runtime struct {
	Metadata Metadata
	Objects  dofs.ObjectStore
	Keys     *dofs.EnvelopeKeyProvider
}

func OpenRuntime(ctx context.Context, cfg config.Config, options Options) (*Runtime, error) {
	metadata, err := OpenMetadata(ctx, cfg, options)
	if err != nil {
		return nil, err
	}
	objects, err := OpenObjectStore(cfg)
	if err != nil {
		_ = metadata.Close()
		return nil, err
	}
	keys, err := OpenKeyProvider(cfg)
	if err != nil {
		_ = metadata.Close()
		return nil, err
	}
	return &Runtime{Metadata: metadata, Objects: objects, Keys: keys}, nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	if r.Keys != nil {
		r.Keys.Close()
	}
	if r.Metadata != nil {
		return r.Metadata.Close()
	}
	return nil
}

func (r *Runtime) Check(ctx context.Context) error {
	if r == nil || r.Metadata == nil || r.Objects == nil || r.Keys == nil {
		return errors.New("DOFS runtime is incomplete")
	}
	if err := r.checkObjectStore(ctx); err != nil {
		return err
	}
	namespaces, err := r.Metadata.ListNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}
	for _, namespace := range namespaces {
		if _, err := r.Metadata.GetNode(ctx, namespace.ID, dofs.RootInode); err != nil {
			return fmt.Errorf("check namespace %s root: %w", namespace.ID, err)
		}
		key, err := r.Keys.UnwrapNamespaceKey(ctx, namespace)
		if err != nil {
			return err
		}
		dofs.Clear(key)
	}
	return nil
}

func (r *Runtime) checkObjectStore(ctx context.Context) error {
	if r == nil || r.Objects == nil {
		return errors.New("DOFS object store is unavailable")
	}
	if checker, ok := r.Objects.(interface{ Check(context.Context) error }); ok {
		if err := checker.Check(ctx); err != nil {
			return fmt.Errorf("check object store: %w", err)
		}
	}
	return nil
}

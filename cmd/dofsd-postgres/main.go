package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/willvar/dofs/cli"
	dofspostgres "github.com/willvar/dofs/metadata/postgres"
	"github.com/willvar/dofs/service"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	options := cli.Options{Service: service.Options{OpenPostgres: openPostgres}}
	if err := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, options); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "dofsd: %v\n", err)
		os.Exit(1)
	}
}

func openPostgres(ctx context.Context, dsn string) (service.Metadata, error) {
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open DOFS PostgreSQL database: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("connect DOFS PostgreSQL database: %w", err)
	}
	store, err := dofspostgres.New(database)
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	return &ownedPostgres{Store: store, database: database}, nil
}

type ownedPostgres struct {
	*dofspostgres.Store
	database *sql.DB
}

func (s *ownedPostgres) Close() error {
	if s == nil || s.database == nil {
		return nil
	}
	err := s.database.Close()
	s.database = nil
	return err
}

var _ service.Metadata = (*ownedPostgres)(nil)

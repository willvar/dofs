module github.com/willvar/dofs/integration/postgres

go 1.26.1

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/willvar/dofs v0.0.0
)

require (
	github.com/hanwen/go-fuse/v2 v2.11.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/willvar/dofs => ../..

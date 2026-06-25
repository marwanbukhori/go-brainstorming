// Package store wraps the Postgres connection pool and DB-enforced invariants
// for the fuel-POS money core. All money invariants (one-active-txn-per-pump,
// idempotency-key uniqueness, double-entry balance) are enforced in Postgres,
// so integration tests run against a real database, never a mock.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/golang-migrate/migrate/v4"
	pgxv5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// migrationsFS embeds the migrations directory. The glob uses the directory
// itself (not *.sql) so the embed compiles even before any SQL files exist —
// Phase 0 has only a .keep placeholder; Phase 1 adds 0001_*.sql files.
//
//go:embed migrations
var migrationsFS embed.FS

// Store wraps the pgx connection pool.
type Store struct {
	Pool *pgxpool.Pool
}

// ErrVersionConflict is returned when an optimistic version-CAS update affects
// zero rows because a concurrent writer won.
var ErrVersionConflict = errors.New("optimistic version conflict")

// ErrNotFound is returned when a transaction row does not exist.
var ErrNotFound = errors.New("transaction not found")

// New opens a pgx connection pool against dsn and verifies connectivity.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{Pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() {
	if s.Pool != nil {
		s.Pool.Close()
	}
}

// Migrate runs all up migrations embedded under migrations/ to the latest
// version using golang-migrate over an iofs source and the pgx/v5 driver.
//
// NOTE (Phase 0): the migrations directory contains no SQL files yet — the
// first migration (0001_transactions) lands in Phase 1. Do not call Migrate()
// before a migration exists; golang-migrate returns "no migration found" /
// ErrNilVersion against an empty source.
func (s *Store) Migrate() error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("store: migrate source: %w", err)
	}
	driver, err := pgxv5.WithInstance(stdlibFromPool(s.Pool), &pgxv5.Config{})
	if err != nil {
		return fmt.Errorf("store: migrate driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("store: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		// Phase 0: migrations directory exists but contains no SQL files yet.
		// iofs.First() returns fs.ErrNotExist when the source has zero migrations;
		// treat that as "nothing to run" rather than a hard error.
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) && errors.Is(pathErr.Err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("store: migrate up: %w", err)
	}
	return nil
}

// stdlibFromPool opens a database/sql DB from the pool's connection config so
// the golang-migrate pgx/v5 driver (which requires *sql.DB) can run against the
// same target database the pool is connected to.
func stdlibFromPool(pool *pgxpool.Pool) *sql.DB {
	cfg := pool.Config().ConnConfig
	return stdlib.OpenDB(*cfg)
}

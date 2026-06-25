package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres spins a throwaway postgres:16 and returns a migrated Store.
// Shared helper for all Phase 2 integration tests in package store_test.
func startPostgres(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()

	pg, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("fuelpos"),
		postgres.WithUsername("fuelpos"),
		postgres.WithPassword("fuelpos"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	s, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)

	if err := s.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func TestIdempotencyKeysTableExists(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	var regclass *string
	err := s.Pool.QueryRow(ctx, `SELECT to_regclass('idempotency_keys')::text`).Scan(&regclass)
	if err != nil {
		t.Fatalf("query to_regclass: %v", err)
	}
	if regclass == nil || *regclass != "idempotency_keys" {
		t.Fatalf("idempotency_keys table missing after Migrate(); to_regclass=%v", regclass)
	}

	// The PRIMARY KEY on key is the dedupe mechanism (ADR-001): assert it exists.
	var n int
	err = s.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.table_constraints
		WHERE table_name = 'idempotency_keys' AND constraint_type = 'PRIMARY KEY'`).Scan(&n)
	if err != nil {
		t.Fatalf("query pk: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 PRIMARY KEY on idempotency_keys, got %d", n)
	}
}

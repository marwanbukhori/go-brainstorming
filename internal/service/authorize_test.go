package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/service"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newAuthorizer(t *testing.T) (*service.Authorizer, *store.Store) {
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
	return service.NewAuthorizer(s), s
}

func TestAuthorize_SameKeyTwice_OneRowSameID(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)

	req := service.AuthorizeRequest{
		IdempotencyKey: "pump-3-authorize-001",
		RequestHash:    "hash-v1",
		PumpID:         "pump-3",
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(15000), // RM150.00
	}

	id1, err := a.Authorize(ctx, req)
	if err != nil {
		t.Fatalf("first authorize: %v", err)
	}

	// Replay the identical request (same idempotency key): must dedupe to the same id.
	id2, err := a.Authorize(ctx, req)
	if err != nil {
		t.Fatalf("second authorize (replay): %v", err)
	}
	if id1 != id2 {
		t.Fatalf("replay returned different id: first=%s second=%s", id1, id2)
	}

	// Exactly ONE transaction row exists for this pump: the effect ran once.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE pump_id = $1`, req.PumpID).Scan(&n); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if n != 1 {
		t.Fatalf("want exactly 1 transaction row, got %d", n)
	}

	// Exactly ONE idempotency_keys row was claimed, bound to the BUSINESS key.
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, req.IdempotencyKey).Scan(&n); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if n != 1 {
		t.Fatalf("want exactly 1 idempotency_keys row, got %d", n)
	}

	// The persisted transactions.idempotency_key MUST be the business key
	// (req.IdempotencyKey), proving the effect bound to the dedupe key, not the row id.
	var storedKey string
	if err := s.Pool.QueryRow(ctx,
		`SELECT idempotency_key FROM transactions WHERE id = $1`, id1).Scan(&storedKey); err != nil {
		t.Fatalf("read stored idempotency_key: %v", err)
	}
	if storedKey != req.IdempotencyKey {
		t.Fatalf("transactions.idempotency_key = %q, want the business key %q", storedKey, req.IdempotencyKey)
	}
}

func TestAuthorize_DifferentKey_NewRow(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)

	req1 := service.AuthorizeRequest{
		IdempotencyKey: "pump-1-authorize-001",
		RequestHash:    "hash-v1",
		PumpID:         "pump-1",
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(10000),
	}
	req2 := service.AuthorizeRequest{
		IdempotencyKey: "pump-2-authorize-001",
		RequestHash:    "hash-v1",
		PumpID:         "pump-2",
		FuelGrade:      "RON97",
		AuthAmount:     money.Amount(20000),
	}

	id1, err := a.Authorize(ctx, req1)
	if err != nil {
		t.Fatalf("first authorize: %v", err)
	}
	id2, err := a.Authorize(ctx, req2)
	if err != nil {
		t.Fatalf("second authorize: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("different keys returned same id: %s", id1)
	}

	// Two distinct rows must exist.
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&n); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 transaction rows, got %d", n)
	}
}

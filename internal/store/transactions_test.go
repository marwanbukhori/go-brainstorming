package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgxv5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// newTestStore spins a throwaway postgres:16 container, opens a Store against
// it, runs all migrations, and registers cleanup. It skips the test if Docker
// is unavailable.
func newTestStore(ctx context.Context, t *testing.T) *Store {
	t.Helper()
	ctr, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("fuelpos"),
		tcpostgres.WithUsername("fuelpos"),
		tcpostgres.WithPassword("fuelpos"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("could not start postgres container (is Docker running?): %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

func newTxn(pumpID string) *transaction.Transaction {
	return &transaction.Transaction{
		ID:             uuid.New(),
		PumpID:         pumpID,
		Status:         transaction.StatusAuthorizing,
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(15000),
		CapturedAmount: money.Amount(0),
		VolumeML:       0,
		AcquirerRef:    "",
		IdempotencyKey: uuid.NewString(),
	}
}

// tableExists reports whether the named table is present in the public schema.
func tableExists(ctx context.Context, t *testing.T, s *Store, name string) bool {
	t.Helper()
	var ok bool
	err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=$1)`,
		name,
	).Scan(&ok)
	if err != nil {
		t.Fatalf("query table existence for %q: %v", name, err)
	}
	return ok
}

func TestMigrateRunsClean(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	// Migrate() already ran inside newTestStore; calling it again must be a no-op.
	if err := s.Migrate(); err != nil {
		t.Fatalf("second Migrate() should be a clean no-op, got: %v", err)
	}
	if !tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table was not created by Migrate()")
	}
}

// newMigrator builds a golang-migrate instance using the embedded iofs source
// and the store's own pool (via a stdlib bridge), exactly like Migrate() does.
// The caller is responsible for calling m.Close().
func newMigrator(t *testing.T, s *Store) *migrate.Migrate {
	t.Helper()
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("iofs.New: %v", err)
	}
	driver, err := pgxv5.WithInstance(stdlibFromPool(s.Pool), &pgxv5.Config{})
	if err != nil {
		t.Fatalf("pgxv5.WithInstance: %v", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		t.Fatalf("migrate.NewWithInstance: %v", err)
	}
	return m
}

// TestMigrateRedo exercises the .down.sql files: migrate fully Down (dropping
// every table) then Up again, asserting the schema is restored. This proves the
// 0001 down migration actually drops the transactions table and that a fresh Up
// rebuilds it (a one-way migration that silently no-ops on Down would pass the
// happy path but fail here).
func TestMigrateRedo(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	// Sanity: schema is up after newTestStore.
	if !tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table missing before redo")
	}

	m := newMigrator(t, s)
	defer m.Close()

	// Down to a clean slate: the transactions table must be gone.
	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate Down: %v", err)
	}
	if tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table still present after migrate Down; 0001 down.sql did not drop it")
	}

	// Up again: the schema must be fully restored.
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate Up after Down: %v", err)
	}
	if !tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table not restored after Down then Up")
	}

	// And it must be usable again (a re-created, empty table).
	in := newTxn("pump-redo-1")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction after redo: %v", err)
	}
}

func TestCreateAndGetTransaction(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-1")
	before := time.Now().Add(-time.Minute)
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	if in.Version != 1 {
		t.Errorf("after create, Version = %d, want 1", in.Version)
	}
	if in.CreatedAt.Before(before) || in.CreatedAt.IsZero() {
		t.Errorf("CreatedAt not back-filled: %v", in.CreatedAt)
	}

	got, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if got.ID != in.ID {
		t.Errorf("ID = %v, want %v", got.ID, in.ID)
	}
	if got.PumpID != "pump-1" {
		t.Errorf("PumpID = %q, want pump-1", got.PumpID)
	}
	if got.Status != transaction.StatusAuthorizing {
		t.Errorf("Status = %q, want AUTHORIZING", got.Status)
	}
	if got.AuthAmount != money.Amount(15000) {
		t.Errorf("AuthAmount = %d, want 15000", int64(got.AuthAmount))
	}
	if got.IdempotencyKey != in.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, in.IdempotencyKey)
	}
	if got.Version != 1 {
		t.Errorf("Version = %d, want 1", got.Version)
	}
}

func TestGetTransactionNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	_, err := s.GetTransaction(ctx, uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetTransaction(missing) err = %v, want ErrNotFound", err)
	}
}

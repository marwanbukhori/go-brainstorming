package store

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres spins a real postgres:16 container and returns its DSN.
// Shared by all store integration tests.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx,
		"postgres:16",
		tcpostgres.WithDatabase("fuelpos"),
		tcpostgres.WithUsername("fuelpos"),
		tcpostgres.WithPassword("fuelpos"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pg.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func TestStoreNewAndPing(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(t)

	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if s.Pool == nil {
		t.Fatal("Store.Pool is nil after New")
	}
	if err := s.Pool.Ping(ctx); err != nil {
		t.Fatalf("Pool.Ping: %v", err)
	}
}

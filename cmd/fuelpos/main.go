// Command fuelpos is the local entrypoint for the fuel-POS money core: it reads
// DATABASE_URL, opens the store, runs migrations to the latest version, and logs
// "ready". It grows in later plans; for now it proves the wiring boots.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/marwanbukhori/go-brainstorming/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is not set")
		os.Exit(1)
	}

	ctx := context.Background()

	s, err := store.New(ctx, dsn)
	if err != nil {
		logger.Error("connect store", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	if err := s.Migrate(); err != nil {
		logger.Error("run migrations", "err", err)
		os.Exit(1)
	}

	logger.Info("ready", "component", "fuelpos")
}

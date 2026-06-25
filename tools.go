//go:build tools

// Package tools pins build/test dependencies that are not yet imported by
// production code, so `go mod tidy` keeps them in go.mod/go.sum. Remove the
// corresponding import once real code (or tests) imports the package.
package tools

import (
	_ "github.com/golang-migrate/migrate/v4"
	_ "github.com/google/uuid"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/testcontainers/testcontainers-go"
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"
)

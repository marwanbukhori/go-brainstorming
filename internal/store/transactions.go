package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// CreateTransactionTx is the SINGLE insert implementation. It binds ALL columns
// (including t.IdempotencyKey -> idempotency_key) and writes version = 1, using
// the caller's tx. On success the DB-assigned version/timestamps are written
// back into t. Callers that must insert the transaction in the same tx as
// another effect (Phase 2 idempotency: claim key + insert txn atomically) call
// this directly; the ctx-only CreateTransaction wraps it with BEGIN/COMMIT.
func (s *Store) CreateTransactionTx(ctx context.Context, tx pgx.Tx, t *transaction.Transaction) error {
	const q = `
INSERT INTO transactions (
    id, pump_id, status, fuel_grade,
    auth_amount_minor, captured_amount_minor, volume_ml,
    acquirer_ref, idempotency_key, version
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1)
RETURNING version, created_at, updated_at`
	return tx.QueryRow(ctx, q,
		t.ID, t.PumpID, string(t.Status), t.FuelGrade,
		int64(t.AuthAmount), int64(t.CapturedAmount), t.VolumeML,
		t.AcquirerRef, t.IdempotencyKey,
	).Scan(&t.Version, &t.CreatedAt, &t.UpdatedAt)
}

// CreateTransaction is the ctx-only wrapper around CreateTransactionTx: it
// BEGINs a transaction, inserts via the single core, and COMMITs (rolling back
// on error). The insert SQL exists only in CreateTransactionTx.
func (s *Store) CreateTransaction(ctx context.Context, t *transaction.Transaction) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := s.CreateTransactionTx(ctx, tx, t); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// GetTransaction loads a single transaction by id (all columns), or ErrNotFound.
// Not-found is detected via errors.Is(err, pgx.ErrNoRows).
func (s *Store) GetTransaction(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	const q = `
SELECT id, pump_id, status, fuel_grade,
       auth_amount_minor, captured_amount_minor, volume_ml,
       acquirer_ref, idempotency_key, version, created_at, updated_at
FROM transactions
WHERE id = $1`

	var (
		t       transaction.Transaction
		status  string
		authMin int64
		capMin  int64
	)
	err := s.Pool.QueryRow(ctx, q, id).Scan(
		&t.ID, &t.PumpID, &status, &t.FuelGrade,
		&authMin, &capMin, &t.VolumeML,
		&t.AcquirerRef, &t.IdempotencyKey, &t.Version, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.Status = transaction.Status(status)
	t.AuthAmount = money.Amount(authMin)
	t.CapturedAmount = money.Amount(capMin)
	return &t, nil
}

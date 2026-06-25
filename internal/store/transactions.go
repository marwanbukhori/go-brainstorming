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

// ApplyTransitionTx is the SINGLE version-CAS implementation. Using the caller's
// tx it loads the row (ALL columns; not-found via errors.Is(err, pgx.ErrNoRows)
// returning ErrNotFound), computes the next status via transaction.Apply
// (returns ErrIllegalTransition if the event is not legal), lets mutate() adjust
// amount/acquirer fields, then performs the ONLY copy of the optimistic CAS:
//
//	UPDATE transactions SET status=$next, version=version+1, ... WHERE id=$id AND version=$expectedVersion
//
// Returns ErrVersionConflict if 0 rows were affected (a concurrent writer won).
// On success returns the updated transaction with Version == expectedVersion+1.
// Phase 3's capture path calls this directly to fold the state change into the
// same tx as the ledger posting. The ctx-only ApplyTransition wraps it with
// BEGIN/COMMIT.
func (s *Store) ApplyTransitionTx(
	ctx context.Context,
	tx pgx.Tx,
	id uuid.UUID,
	expectedVersion int64,
	e transaction.Event,
	mutate func(*transaction.Transaction),
) (*transaction.Transaction, error) {
	// Load the row only if it exists AND is still at expectedVersion. If the
	// version has already been advanced by a concurrent writer the SELECT returns
	// 0 rows — we surface that as ErrVersionConflict immediately, before the
	// state-machine check, so callers never see ErrIllegalTransition due to a
	// race. We distinguish "row never existed" (ErrNotFound) from "row exists
	// but version mismatch" (ErrVersionConflict) via a second point-lookup.
	const loadQ = `
SELECT id, pump_id, status, fuel_grade,
       auth_amount_minor, captured_amount_minor, volume_ml,
       acquirer_ref, idempotency_key, version, created_at, updated_at
FROM transactions
WHERE id = $1 AND version = $2`

	var (
		current transaction.Transaction
		status  string
		authMin int64
		capMin  int64
	)
	err := tx.QueryRow(ctx, loadQ, id, expectedVersion).Scan(
		&current.ID, &current.PumpID, &status, &current.FuelGrade,
		&authMin, &capMin, &current.VolumeML,
		&current.AcquirerRef, &current.IdempotencyKey, &current.Version,
		&current.CreatedAt, &current.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// Could be not-found or version mismatch — distinguish with a point-lookup.
		var exists bool
		if scanErr := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM transactions WHERE id = $1)`, id,
		).Scan(&exists); scanErr != nil {
			return nil, scanErr
		}
		if !exists {
			return nil, ErrNotFound
		}
		return nil, ErrVersionConflict
	}
	if err != nil {
		return nil, err
	}
	current.Status = transaction.Status(status)
	current.AuthAmount = money.Amount(authMin)
	current.CapturedAmount = money.Amount(capMin)

	next, err := transaction.Apply(current.Status, e)
	if err != nil {
		return nil, err
	}

	current.Status = next
	if mutate != nil {
		mutate(&current)
	}

	const updateQ = `
UPDATE transactions
SET status                = $1,
    fuel_grade            = $2,
    auth_amount_minor     = $3,
    captured_amount_minor = $4,
    volume_ml             = $5,
    acquirer_ref          = $6,
    version               = version + 1,
    updated_at            = now()
WHERE id = $7 AND version = $8
RETURNING version, updated_at`

	err = tx.QueryRow(ctx, updateQ,
		string(current.Status), current.FuelGrade,
		int64(current.AuthAmount), int64(current.CapturedAmount), current.VolumeML,
		current.AcquirerRef,
		id, expectedVersion,
	).Scan(&current.Version, &current.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrVersionConflict
	}
	if err != nil {
		return nil, err
	}
	return &current, nil
}

// ApplyTransition is the ctx-only wrapper around ApplyTransitionTx: it BEGINs a
// transaction, runs the single CAS core, and COMMITs (rolling back on error).
// The WHERE id=$id AND version=$expectedVersion SQL exists ONLY in
// ApplyTransitionTx; this method has none.
func (s *Store) ApplyTransition(
	ctx context.Context,
	id uuid.UUID,
	expectedVersion int64,
	e transaction.Event,
	mutate func(*transaction.Transaction),
) (*transaction.Transaction, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	out, err := s.ApplyTransitionTx(ctx, tx, id, expectedVersion, e, mutate)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
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

package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// errInjectedCrash simulates a process crash (or any error) after the claim and
// the effect were written, but BEFORE COMMIT. Under ADR-001 the rollback that
// follows must discard BOTH writes atomically.
var errInjectedCrash = errors.New("injected crash before commit")

func TestClaimThenRollback_LeavesNeitherKeyNorTransaction(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	key := "pump-9-authorize-crash"
	txnID := uuid.New()

	// One tx: claim the key, write the effect, then DO NOT commit — roll back.
	err := func() error {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		// Guarantees a rollback on every return path of this closure.
		defer func() { _ = tx.Rollback(ctx) }()

		_, claimed, err := s.ClaimIdempotencyKey(ctx, tx, key, "hash-v1", txnID)
		if err != nil {
			return err
		}
		if !claimed {
			t.Fatalf("setup: expected to win the claim on a fresh db")
		}

		// The effect: persist the transaction via the canonical tx-aware insert,
		// inside the SAME tx as the claim (ADR-001).
		if err := s.CreateTransactionTx(ctx, tx, &transaction.Transaction{
			ID:             txnID,
			PumpID:         "pump-9",
			Status:         transaction.StatusAuthorizing,
			FuelGrade:      "RON95",
			AuthAmount:     money.Amount(15000),
			IdempotencyKey: key,
		}); err != nil {
			return err
		}

		// Crash before COMMIT: return an error so the deferred Rollback fires.
		return errInjectedCrash
	}()
	if !errors.Is(err, errInjectedCrash) {
		t.Fatalf("want injected crash error to propagate, got %v", err)
	}

	// PROOF 1: the idempotency_keys row is ABSENT — the claim did not persist.
	// Query uses the pool (a separate connection), NOT the rolled-back tx.
	var keys int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&keys); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if keys != 0 {
		t.Fatalf("want 0 idempotency_keys rows after rollback, got %d", keys)
	}

	// PROOF 2: no transactions row exists — the effect did not persist either.
	// GetTransaction uses s.Pool internally (separate connection from the dead tx).
	if _, err := s.GetTransaction(ctx, txnID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound for rolled-back transaction, got %v", err)
	}
}

// TestClaimThenCommit_PersistsBothRowsContrast is the positive contrast:
// a committed claim+effect DOES persist both the idempotency_keys row and the
// transactions row. This confirms the test is not vacuously passing because the
// store is broken — it asserts the happy path works before proving the sad path.
func TestClaimThenCommit_PersistsBothRowsContrast(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	key := "pump-9-authorize-commit"
	txnID := uuid.New()

	// One tx: claim the key, write the effect, then COMMIT.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, claimed, err := s.ClaimIdempotencyKey(ctx, tx, key, "hash-v1", txnID)
	if err != nil {
		t.Fatalf("ClaimIdempotencyKey: %v", err)
	}
	if !claimed {
		t.Fatalf("setup: expected to win the claim on a fresh db")
	}

	if err := s.CreateTransactionTx(ctx, tx, &transaction.Transaction{
		ID:             txnID,
		PumpID:         "pump-9",
		Status:         transaction.StatusAuthorizing,
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(15000),
		IdempotencyKey: key,
	}); err != nil {
		t.Fatalf("CreateTransactionTx: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// ASSERT 1: the idempotency_keys row IS present after commit.
	var keys int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&keys); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if keys != 1 {
		t.Fatalf("want 1 idempotency_keys row after commit, got %d", keys)
	}

	// ASSERT 2: the transactions row IS present after commit.
	got, err := s.GetTransaction(ctx, txnID)
	if err != nil {
		t.Fatalf("GetTransaction after commit: %v", err)
	}
	if got.ID != txnID {
		t.Fatalf("want txnID %s, got %s", txnID, got.ID)
	}
	if got.IdempotencyKey != key {
		t.Fatalf("want idempotency key %q, got %q", key, got.IdempotencyKey)
	}
}

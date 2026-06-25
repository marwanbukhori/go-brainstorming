package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/service"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// driveToCompleted walks a freshly-authorized transaction to COMPLETED through
// the REAL state machine via the store's version-CAS ApplyTransition, exercising
// the true transition path (no raw UPDATE ... SET status). It returns the id and
// the version at COMPLETED.
func driveToCompleted(ctx context.Context, t *testing.T, a *service.Authorizer, s *store.Store, key, pump string, auth money.Amount) (uuid.UUID, int64) {
	t.Helper()
	id, err := a.Authorize(ctx, service.AuthorizeRequest{
		IdempotencyKey: key,
		RequestHash:    "hash-v1",
		PumpID:         pump,
		FuelGrade:      "RON95",
		AuthAmount:     auth,
	})
	if err != nil {
		t.Fatalf("seed authorize: %v", err)
	}

	// Authorize already created the row at AUTHORIZING (version 1) via the real
	// authorize path. Walk it forward through the genuine transition table:
	//   AUTHORIZING --AcquirerApproved--> AUTHORIZED
	//   AUTHORIZED  --StartDispense-----> DISPENSING
	//   DISPENSING  --PumpStopped-------> COMPLETED
	version := int64(1)
	for _, e := range []transaction.Event{
		transaction.EventAcquirerApproved,
		transaction.EventStartDispense,
		transaction.EventPumpStopped,
	} {
		tr, err := s.ApplyTransition(ctx, id, version, e, nil)
		if err != nil {
			t.Fatalf("ApplyTransition(%s): %v", e, err)
		}
		version = tr.Version
	}
	return id, version
}

func TestCapture_PostsStateAndLedgerInOneTx_InvariantHolds(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)
	cap := service.NewCapturer(s)

	// Drive to COMPLETED through the REAL state machine. AuthAmount RM200.00.
	id, version := driveToCompleted(ctx, t, a, s, "pump-5-capture-seed", "pump-5", money.Amount(20000))

	// The actual amount dispensed is LOWER than the pre-auth (capture the lower actual).
	captured := money.Amount(17500) // RM175.00

	// THE DEMONSTRATION: one Capture call does COMPLETED->CAPTURING->CAPTURED AND
	// the ledger post in one tx, reusing the store's ApplyTransitionTx version-CAS.
	got, err := cap.Capture(ctx, id, version, captured)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if got.Status != transaction.StatusCaptured {
		t.Fatalf("status = %q, want CAPTURED", got.Status)
	}
	if got.CapturedAmount != captured {
		t.Fatalf("captured_amount = %d, want %d", int64(got.CapturedAmount), int64(captured))
	}

	// Persisted: the row is CAPTURED.
	var status string
	if err := s.Pool.QueryRow(ctx,
		`SELECT status FROM transactions WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if status != "CAPTURED" {
		t.Fatalf("persisted status = %q, want CAPTURED", status)
	}

	// THE INVARIANT (ADR-002): after the capture, the global fold balances.
	debits, credits, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant: %v", err)
	}
	if debits != credits {
		t.Fatalf("invariant broken after capture: debits=%d credits=%d", int64(debits), int64(credits))
	}
	if debits != captured {
		t.Fatalf("debits = %d, want the captured amount %d", int64(debits), int64(captured))
	}

	// Exactly one balanced pair of ledger entries was posted for this txn.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 ledger entries after capture, got %d", n)
	}
}

func TestCapture_ExceedsAuth_RejectedAndPostsNothing(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)
	cap := service.NewCapturer(s)

	// AuthAmount RM150.00; attempt to capture RM150.01 (one sen over) — a HARD
	// guard violation (captured > auth) per spec §8 / ADR money rules.
	id, version := driveToCompleted(ctx, t, a, s, "pump-7-over-capture", "pump-7", money.Amount(15000))

	_, err := cap.Capture(ctx, id, version, money.Amount(15001))
	if !errors.Is(err, store.ErrCaptureExceedsAuth) {
		t.Fatalf("Capture(over-auth): want ErrCaptureExceedsAuth, got %v", err)
	}

	// Status UNCHANGED: still COMPLETED (the transition was rolled back).
	var status string
	var gotVersion int64
	if err := s.Pool.QueryRow(ctx,
		`SELECT status, version FROM transactions WHERE id = $1`, id).Scan(&status, &gotVersion); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if status != "COMPLETED" {
		t.Fatalf("status = %q, want COMPLETED (unchanged after rejected capture)", status)
	}
	if gotVersion != version {
		t.Fatalf("version = %d, want %d (unchanged after rejected capture)", gotVersion, version)
	}

	// NOTHING posted to the ledger for this txn.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 ledger entries after a rejected over-auth capture, got %d", n)
	}
}

func TestCapture_EqualToAuth_Allowed(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)
	cap := service.NewCapturer(s)

	// BOUNDARY: captured == auth is allowed (the guard is captured <= auth).
	auth := money.Amount(15000)
	id, version := driveToCompleted(ctx, t, a, s, "pump-8-equal-capture", "pump-8", auth)

	got, err := cap.Capture(ctx, id, version, auth)
	if err != nil {
		t.Fatalf("Capture(captured==auth): want success, got %v", err)
	}
	if got.Status != transaction.StatusCaptured {
		t.Fatalf("status = %q, want CAPTURED", got.Status)
	}
	if got.CapturedAmount != auth {
		t.Fatalf("captured_amount = %d, want %d", int64(got.CapturedAmount), int64(auth))
	}

	// The balanced pair was posted at exactly the auth amount.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 ledger entries after boundary capture, got %d", n)
	}
}

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// TestConcurrentApplyTransitionExactlyOneWinner fires two conflicting
// transitions at the same row, both claiming expectedVersion 1. Exactly one
// must succeed (version -> 2); the other must get ErrVersionConflict. This is a
// regression test that guards Task 5's version-CAS under real concurrency.
func TestConcurrentApplyTransitionExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-race-1")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}

	// Two legal-from-AUTHORIZING events racing on version 1.
	events := []transaction.Event{
		transaction.EventAcquirerApproved, // -> AUTHORIZED
		transaction.EventAcquirerDeclined, // -> DECLINED
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		conflicts int
	)
	wg.Add(len(events))
	for _, e := range events {
		e := e
		go func() {
			defer wg.Done()
			_, err := s.ApplyTransition(ctx, in.ID, 1, e, func(*transaction.Transaction) {})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrVersionConflict):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Errorf("successes = %d, want exactly 1", successes)
	}
	if conflicts != 1 {
		t.Errorf("conflicts = %d, want exactly 1", conflicts)
	}

	final, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if final.Version != 2 {
		t.Errorf("final Version = %d, want 2 (exactly one writer advanced it)", final.Version)
	}
}

// TestSecondActiveTxnPerPumpViolatesIndex guards ADR-004: a second live
// (non-terminal) transaction for a pump that already has a live transaction is
// rejected by the one_active_txn_per_pump partial unique index.
func TestSecondActiveTxnPerPumpViolatesIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	first := newTxn("pump-serial-1") // AUTHORIZING (non-terminal => live)
	if err := s.CreateTransaction(ctx, first); err != nil {
		t.Fatalf("first CreateTransaction: %v", err)
	}

	second := newTxn("pump-serial-1") // same pump, also live
	err := s.CreateTransaction(ctx, second)
	if err == nil {
		t.Fatal("second live transaction for the same pump was accepted; want unique violation")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("want unique_violation (SQLSTATE 23505) on one_active_txn_per_pump, got: %v", err)
	}
	if !strings.Contains(pgErr.ConstraintName, "one_active_txn_per_pump") {
		t.Errorf("violated constraint = %q, want one_active_txn_per_pump", pgErr.ConstraintName)
	}

	// Drive the first transaction to a terminal state, then a new live txn for
	// the same pump must be allowed (the partial index only covers live rows).
	if _, err := s.ApplyTransition(ctx, first.ID, 1, transaction.EventAcquirerDeclined, func(*transaction.Transaction) {}); err != nil {
		t.Fatalf("decline first txn: %v", err)
	}
	third := newTxn("pump-serial-1")
	if err := s.CreateTransaction(ctx, third); err != nil {
		t.Errorf("after first txn terminal, a new live txn for the pump should be allowed, got: %v", err)
	}
}

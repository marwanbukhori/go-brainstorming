package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

func TestApplyTransitionHappyPath(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-apply-1")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}

	// AUTHORIZING --AcquirerApproved--> AUTHORIZED, recording the acquirer ref.
	out, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventAcquirerApproved,
		func(tx *transaction.Transaction) { tx.AcquirerRef = "auth-ref-9" })
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	if out.Status != transaction.StatusAuthorized {
		t.Errorf("Status = %q, want AUTHORIZED", out.Status)
	}
	if out.Version != 2 {
		t.Errorf("Version = %d, want 2", out.Version)
	}
	if out.AcquirerRef != "auth-ref-9" {
		t.Errorf("AcquirerRef = %q, want auth-ref-9", out.AcquirerRef)
	}

	reloaded, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if reloaded.Status != transaction.StatusAuthorized || reloaded.Version != 2 {
		t.Errorf("persisted = (%q,v%d), want (AUTHORIZED,v2)", reloaded.Status, reloaded.Version)
	}
}

func TestApplyTransitionStaleVersionConflict(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-apply-2")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	// First transition wins, moving version 1 -> 2.
	if _, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventAcquirerApproved, func(*transaction.Transaction) {}); err != nil {
		t.Fatalf("first ApplyTransition: %v", err)
	}
	// Second call still claims expectedVersion 1 -> must conflict.
	_, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventStartDispense, func(*transaction.Transaction) {})
	if !errors.Is(err, ErrVersionConflict) {
		t.Errorf("stale ApplyTransition err = %v, want ErrVersionConflict", err)
	}
}

func TestApplyTransitionIllegalEvent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-apply-3")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	// AUTHORIZING --Capture--> illegal.
	_, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventCapture, func(*transaction.Transaction) {})
	if !errors.Is(err, transaction.ErrIllegalTransition) {
		t.Errorf("illegal ApplyTransition err = %v, want ErrIllegalTransition", err)
	}
	// State must be untouched.
	reloaded, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if reloaded.Status != transaction.StatusAuthorizing || reloaded.Version != 1 {
		t.Errorf("after illegal event = (%q,v%d), want (AUTHORIZING,v1)", reloaded.Status, reloaded.Version)
	}
}

func TestApplyTransitionNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	_, err := s.ApplyTransition(ctx, uuid.New(), 1, transaction.EventAcquirerApproved, func(*transaction.Transaction) {})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("ApplyTransition(missing) err = %v, want ErrNotFound", err)
	}
	_ = money.Amount(0) // money import kept consistent across store tests
}

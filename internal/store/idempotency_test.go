package store_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestClaimIdempotencyKey_FirstClaimsSecondReturnsSnapshot(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	key := "idem-key-abc"
	reqHash := "hash-v1"
	txnID := uuid.New()

	// First claim: inside its own tx, inserts the row and returns claimed=true.
	tx1, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	existing, claimed, err := s.ClaimIdempotencyKey(ctx, tx1, key, reqHash, txnID)
	if err != nil {
		t.Fatalf("first ClaimIdempotencyKey: %v", err)
	}
	if !claimed {
		t.Fatalf("first claim: want claimed=true, got false")
	}
	if existing != nil {
		t.Fatalf("first claim: want existing=nil, got %+v", existing)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	// Second claim, same key, different txnID: row already exists, returns the STORED snapshot.
	tx2, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	existing, claimed, err = s.ClaimIdempotencyKey(ctx, tx2, key, reqHash, uuid.New())
	if err != nil {
		t.Fatalf("second ClaimIdempotencyKey: %v", err)
	}
	if claimed {
		t.Fatalf("second claim: want claimed=false, got true")
	}
	if existing == nil {
		t.Fatalf("second claim: want non-nil snapshot, got nil")
	}
	if existing.TransactionID != txnID {
		t.Fatalf("second claim: snapshot TransactionID = %s, want the first-claim id %s", existing.TransactionID, txnID)
	}
}

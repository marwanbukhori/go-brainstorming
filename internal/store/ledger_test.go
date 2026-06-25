package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func TestPostEntries_BalancedInserts_UnbalancedRejectsNothingInserted(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	// --- Balanced batch inserts both legs in one tx ---
	txnID := uuid.New()
	balanced := []ledger.Entry{
		{TransactionID: txnID, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(15000)},
		{TransactionID: txnID, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(15000)},
	}

	tx1, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	if err := s.PostEntries(ctx, tx1, balanced); err != nil {
		t.Fatalf("PostEntries(balanced): %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, txnID).Scan(&n); err != nil {
		t.Fatalf("count balanced entries: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 ledger entries for the balanced batch, got %d", n)
	}

	// --- Unbalanced batch is rejected with ErrUnbalanced and inserts NOTHING ---
	badTxnID := uuid.New()
	unbalanced := []ledger.Entry{
		{TransactionID: badTxnID, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(15000)},
		{TransactionID: badTxnID, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(14999)},
	}

	tx2, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	err = s.PostEntries(ctx, tx2, unbalanced)
	if !errors.Is(err, ledger.ErrUnbalanced) {
		_ = tx2.Rollback(ctx)
		t.Fatalf("PostEntries(unbalanced): want ErrUnbalanced, got %v", err)
	}
	// Even if the caller went on to commit, the rejected batch wrote nothing.
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2 (after rejected post): %v", err)
	}
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, badTxnID).Scan(&n); err != nil {
		t.Fatalf("count unbalanced entries: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 ledger entries after an unbalanced (rejected) batch, got %d", n)
	}
}

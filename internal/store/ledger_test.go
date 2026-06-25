package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func TestFoldInvariant_DebitsEqualCreditsAfterBatch(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	// Empty ledger: both sums are zero (and trivially equal).
	d0, c0, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant(empty): %v", err)
	}
	if d0 != money.Amount(0) || c0 != money.Amount(0) {
		t.Fatalf("empty ledger: want (0,0), got (%d,%d)", int64(d0), int64(c0))
	}

	// Post two balanced batches (a split-debit one to exercise the fold over many rows).
	txnA := uuid.New()
	txnB := uuid.New()
	batchA := []ledger.Entry{
		{TransactionID: txnA, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(15000)},
		{TransactionID: txnA, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(15000)},
	}
	batchB := []ledger.Entry{
		{TransactionID: txnB, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(500)},
		{TransactionID: txnB, Account: "card-clearing", Direction: ledger.Debit, Amount: money.Amount(2000)},
		{TransactionID: txnB, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(2500)},
	}

	for _, batch := range [][]ledger.Entry{batchA, batchB} {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := s.PostEntries(ctx, tx, batch); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("PostEntries: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	debits, credits, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant: %v", err)
	}
	// The continuous invariant (ADR-002): debits == credits.
	if debits != credits {
		t.Fatalf("invariant broken: debits=%d credits=%d", int64(debits), int64(credits))
	}
	// And concretely: 15000 + (500+2000) = 17500 on each side.
	if debits != money.Amount(17500) {
		t.Fatalf("debits = %d, want 17500", int64(debits))
	}
	if credits != money.Amount(17500) {
		t.Fatalf("credits = %d, want 17500", int64(credits))
	}
}

// TestFoldInvariant_DetectsOutOfBandUnbalancedInsert proves the fold is
// NECESSARY-BUT-NOT-SUFFICIENT: balance is guarded by PostEntries/Balanced, NOT
// by the ledger_entries table CHECK (which only enforces amount_minor > 0 and
// the direction token). A direct, out-of-band INSERT that bypasses PostEntries
// can leave the ledger unbalanced, and FoldInvariant is what surfaces it
// (debits != credits). This is exactly why every legitimate write goes through
// PostEntries — and why the fold exists as the global detector.
func TestFoldInvariant_DetectsOutOfBandUnbalancedInsert(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	// A lone DEBIT inserted directly — this passes the table CHECKs (amount > 0,
	// direction is 'DEBIT') yet has no matching CREDIT. PostEntries/Balanced would
	// have rejected it; the raw INSERT does not.
	badTxn := uuid.New()
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account, direction, amount_minor)
		VALUES ($1, 'cash-clearing', 'DEBIT', 15000)`, badTxn); err != nil {
		t.Fatalf("out-of-band insert: %v", err)
	}

	debits, credits, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant: %v", err)
	}
	// The fold SURFACES the breach: the table CHECK did not (and cannot) prevent it.
	if debits == credits {
		t.Fatalf("expected fold to detect imbalance, got debits=%d credits=%d (equal)", int64(debits), int64(credits))
	}
	if debits != money.Amount(15000) || credits != money.Amount(0) {
		t.Fatalf("want (debits=15000, credits=0), got (%d,%d)", int64(debits), int64(credits))
	}
}

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

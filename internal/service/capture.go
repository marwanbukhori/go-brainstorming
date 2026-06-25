package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// Ledger accounts for the fuel-capture posting. Simple string constants — no
// sharding, no clearing-sweep (those are later plans).
const (
	AccountCashClearing = "cash-clearing"
	AccountFuelRevenue  = "fuel-revenue"
)

// Capturer turns a COMPLETED fuel transaction into a CAPTURED one and posts the
// balanced ledger entries — both in ONE Postgres transaction (ADR-002).
type Capturer struct {
	Store *store.Store
}

// NewCapturer constructs a Capturer.
func NewCapturer(s *store.Store) *Capturer {
	return &Capturer{Store: s}
}

// Capture is the headline ADR-002 demonstration: a single pgx.Tx performs BOTH
// the state transition (COMPLETED -> CAPTURING -> CAPTURED) AND the balanced
// ledger posting (DEBIT cash-clearing / CREDIT fuel-revenue for the captured
// amount). The state transition reuses the store's version-CAS core
// ApplyTransitionTx inside the shared tx — there is ONE version-CAS
// implementation, never a copy. Because both commit atomically, the global
// invariant debits==credits is continuously true — no captured-but-unbooked
// window.
//
// The captured <= auth_amount HARD guard (spec §8 / ADR money rules) is enforced
// here, at the service layer, BEFORE stamping the captured amount: if
// captured > AuthAmount it returns store.ErrCaptureExceedsAuth and posts NOTHING
// — the tx is rolled back and the row's status is left unchanged. The pure state
// machine does not carry amount guards.
//
// expectedVersion is the version of the row at COMPLETED (the caller holds it
// from the prior transition). captured is the actual dispensed amount, which is
// typically lower than the pre-auth.
func (c *Capturer) Capture(ctx context.Context, id uuid.UUID, expectedVersion int64, captured money.Amount) (*transaction.Transaction, error) {
	if captured <= 0 {
		return nil, fmt.Errorf("capture amount must be > 0, got %d", int64(captured))
	}

	tx, err := c.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin capture tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// (1) COMPLETED -> CAPTURING, reusing the store's version-CAS core in this tx.
	t, err := c.Store.ApplyTransitionTx(ctx, tx, id, expectedVersion, transaction.EventCapture, nil)
	if err != nil {
		return nil, fmt.Errorf("transition to CAPTURING: %w", err)
	}

	// HARD GUARD (captured <= auth_amount): enforced at the service layer, before
	// stamping. On violation we return ErrCaptureExceedsAuth and post nothing —
	// the deferred Rollback leaves the row's status unchanged.
	if captured > t.AuthAmount {
		return nil, store.ErrCaptureExceedsAuth
	}

	// (2) CAPTURING -> CAPTURED, stamping the (guarded) captured amount.
	t, err = c.Store.ApplyTransitionTx(ctx, tx, id, t.Version, transaction.EventAcquirerCaptured, func(tr *transaction.Transaction) {
		tr.CapturedAmount = captured
	})
	if err != nil {
		return nil, fmt.Errorf("transition to CAPTURED: %w", err)
	}

	// (3) Post the balanced ledger pair in the SAME tx.
	entries := []ledger.Entry{
		{TransactionID: id, Account: AccountCashClearing, Direction: ledger.Debit, Amount: captured},
		{TransactionID: id, Account: AccountFuelRevenue, Direction: ledger.Credit, Amount: captured},
	}
	if err := c.Store.PostEntries(ctx, tx, entries); err != nil {
		return nil, fmt.Errorf("post capture ledger entries: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit capture: %w", err)
	}
	return t, nil
}

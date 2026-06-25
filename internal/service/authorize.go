package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// AuthorizeRequest is one business intent to authorize a fuel sale.
// IdempotencyKey is the dedupe key; replays carry the same key.
type AuthorizeRequest struct {
	IdempotencyKey string
	RequestHash    string
	PumpID         string
	FuelGrade      string
	AuthAmount     money.Amount
}

// Authorizer wires the ADR-001 atomic-idempotency pattern over the Store.
type Authorizer struct {
	Store *store.Store
}

// NewAuthorizer constructs an Authorizer.
func NewAuthorizer(s *store.Store) *Authorizer {
	return &Authorizer{Store: s}
}

// Authorize realises ADR-001: claim-the-key and commit-the-effect in ONE
// Postgres transaction so a crash can never persist the key without the effect.
//
//	BEGIN
//	  ClaimIdempotencyKey(key)
//	  if claimed:   CreateTransactionTx(...)   -- the effect, SAME tx
//	  else:         return the stored transaction id   -- NO second effect
//	COMMIT
//
// On replay with the same key, the claim conflicts and the stored transaction id
// is returned without re-running the effect. A goroutine that wins its in-tx claim
// but loses the COMMIT race to a concurrent winner resolves the stored id instead
// of erroring.
func (a *Authorizer) Authorize(ctx context.Context, req AuthorizeRequest) (uuid.UUID, error) {
	tx, err := a.Store.Pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin tx: %w", err)
	}
	// Rollback is a no-op after a successful Commit.
	defer func() { _ = tx.Rollback(ctx) }()

	txnID := uuid.New()

	existing, claimed, err := a.Store.ClaimIdempotencyKey(ctx, tx, req.IdempotencyKey, req.RequestHash, txnID)
	if err != nil {
		return uuid.Nil, err
	}
	if !claimed {
		// Duplicate intent: the effect already committed under another tx.
		// Return the stored id; do NOT perform a second effect.
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, fmt.Errorf("commit (dedup): %w", err)
		}
		return existing.TransactionID, nil
	}

	// We own the claim: persist the effect via the canonical tx-aware insert,
	// in the SAME tx as the claim (ADR-001). IdempotencyKey is the BUSINESS key
	// (req.IdempotencyKey) so the stored idempotency_key binds to the dedupe key.
	if err := a.Store.CreateTransactionTx(ctx, tx, &transaction.Transaction{
		ID:             txnID,
		PumpID:         req.PumpID,
		Status:         transaction.StatusAuthorizing,
		FuelGrade:      req.FuelGrade,
		AuthAmount:     req.AuthAmount,
		IdempotencyKey: req.IdempotencyKey,
	}); err != nil {
		return uuid.Nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		// A concurrent winner committed the same key first; our claim was
		// optimistic. Rollback is already deferred; re-resolve the stored id.
		if isUniqueViolation(err) {
			return a.resolveExisting(ctx, req.IdempotencyKey)
		}
		return uuid.Nil, fmt.Errorf("commit (effect): %w", err)
	}
	return txnID, nil
}

// isUniqueViolation reports whether err is a Postgres unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// resolveExisting reads back the committed transaction id for a key whose
// claim we lost to a concurrent winner.
func (a *Authorizer) resolveExisting(ctx context.Context, key string) (uuid.UUID, error) {
	var id uuid.UUID
	err := a.Store.Pool.QueryRow(ctx,
		`SELECT transaction_id FROM idempotency_keys WHERE key = $1`, key).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve existing id: %w", err)
	}
	return id, nil
}

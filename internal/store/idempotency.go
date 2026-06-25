package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// IdempotencySnapshot is the stored result of a previously-claimed key.
// On a duplicate claim the caller returns this instead of re-executing the effect.
type IdempotencySnapshot struct {
	TransactionID uuid.UUID
	ResponseJSON  []byte
}

// ClaimIdempotencyKey attempts INSERT ... ON CONFLICT (key) DO NOTHING within tx
// (ADR-001: the claim must live in the SAME transaction as the effect it guards).
//
//   - If the row was inserted, this caller won the claim: returns (nil, true, nil)
//     and the caller proceeds to perform the effect inside the same tx.
//   - If the key already existed, the effect already happened (or is in flight under
//     another committed tx): returns (existing snapshot, false, nil) and the caller
//     returns the stored result WITHOUT re-executing.
func (s *Store) ClaimIdempotencyKey(ctx context.Context, tx pgx.Tx, key, requestHash string, txnID uuid.UUID) (existing *IdempotencySnapshot, claimed bool, err error) {
	// RETURNING only yields a row when the INSERT actually inserted (no conflict).
	var insertedKey string
	err = tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (key, request_hash, transaction_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO NOTHING
		RETURNING key`,
		key, requestHash, txnID,
	).Scan(&insertedKey)
	if err == nil {
		// We inserted the row: this caller owns the claim.
		return nil, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("claim idempotency key: %w", err)
	}

	// Conflict: the key already exists. Load the stored snapshot.
	snap := &IdempotencySnapshot{}
	err = tx.QueryRow(ctx, `
		SELECT transaction_id, response_snapshot
		FROM idempotency_keys
		WHERE key = $1`,
		key,
	).Scan(&snap.TransactionID, &snap.ResponseJSON)
	if err != nil {
		return nil, false, fmt.Errorf("load existing idempotency snapshot: %w", err)
	}
	return snap, false, nil
}

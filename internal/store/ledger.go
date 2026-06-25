package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
)

// PostEntries validates the batch with ledger.Balanced (per-transaction
// double-entry invariant, ADR-002) BEFORE any write, then bulk-INSERTs every
// entry inside the caller's tx. It is append-only: it never updates or deletes
// (ADR-002/003). Posting in the SAME tx as the state change is the whole point
// of the synchronous plane — there is no captured-but-unbooked window.
//
// On an unbalanced batch it returns ledger.ErrUnbalanced and writes nothing.
func (s *Store) PostEntries(ctx context.Context, tx pgx.Tx, entries []ledger.Entry) error {
	if err := ledger.Balanced(entries); err != nil {
		return err // ledger.ErrUnbalanced — nothing inserted
	}

	batch := &pgx.Batch{}
	for _, e := range entries {
		batch.Queue(`
			INSERT INTO ledger_entries (transaction_id, account, direction, amount_minor)
			VALUES ($1, $2, $3, $4)`,
			e.TransactionID, e.Account, string(e.Direction), int64(e.Amount),
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()
	for range entries {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("post ledger entry: %w", err)
		}
	}
	return nil
}

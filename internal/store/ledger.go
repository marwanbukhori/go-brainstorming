package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
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

// FoldInvariant returns the global sums of DEBIT and CREDIT amounts across the
// whole ledger. The continuous invariant is debits == credits (ADR-002) — true
// at all times because posting is synchronous (PostEntries runs in the same tx
// as the state change). It is expressed as a SUM/fold rather than a balance
// column so it survives the future shard-by-pump migration (ADR-010 framing):
// post-shard each shard folds its own slice and the residuals fold to zero
// globally — the query shape does not change.
//
// The fold is the GLOBAL detector of imbalance. It is necessary-but-not-
// sufficient: the ledger_entries CHECK guards only amount_minor > 0 and the
// direction token, so an out-of-band unbalanced INSERT would pass the table
// CHECK yet make this fold report debits != credits. Balance itself is guarded
// by PostEntries/Balanced; FoldInvariant surfaces any breach.
func (s *Store) FoldInvariant(ctx context.Context) (debits money.Amount, credits money.Amount, err error) {
	var d, c int64
	err = s.Pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0),
			COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)
		FROM ledger_entries`).Scan(&d, &c)
	if err != nil {
		return 0, 0, fmt.Errorf("fold invariant: %w", err)
	}
	return money.Amount(d), money.Amount(c), nil
}

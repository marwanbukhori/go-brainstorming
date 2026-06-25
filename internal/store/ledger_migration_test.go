package store_test

import (
	"context"
	"testing"
)

func TestLedgerEntriesTableExists(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	var regclass *string
	if err := s.Pool.QueryRow(ctx, `SELECT to_regclass('ledger_entries')::text`).Scan(&regclass); err != nil {
		t.Fatalf("query to_regclass: %v", err)
	}
	if regclass == nil || *regclass != "ledger_entries" {
		t.Fatalf("ledger_entries table missing after Migrate(); to_regclass=%v", regclass)
	}

	// ADR-003: amount_minor must reject non-positive amounts via a CHECK constraint.
	// A direct INSERT of a zero amount must be refused by Postgres.
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account, direction, amount_minor)
		VALUES (gen_random_uuid(), 'cash-clearing', 'DEBIT', 0)`)
	if err == nil {
		t.Fatalf("expected CHECK violation inserting amount_minor=0, got nil error")
	}

	// The direction CHECK must reject anything outside ('DEBIT','CREDIT').
	_, err = s.Pool.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account, direction, amount_minor)
		VALUES (gen_random_uuid(), 'cash-clearing', 'SIDEWAYS', 100)`)
	if err == nil {
		t.Fatalf("expected CHECK violation inserting direction='SIDEWAYS', got nil error")
	}

	// The per-transaction index must exist (ADR-003 query path: entries by txn).
	var idx *string
	if err := s.Pool.QueryRow(ctx, `SELECT to_regclass('ledger_entries_txn_idx')::text`).Scan(&idx); err != nil {
		t.Fatalf("query index to_regclass: %v", err)
	}
	if idx == nil || *idx != "ledger_entries_txn_idx" {
		t.Fatalf("ledger_entries_txn_idx missing after Migrate(); to_regclass=%v", idx)
	}
}

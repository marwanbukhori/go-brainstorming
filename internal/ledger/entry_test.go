package ledger_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func TestEntry_FieldsAndDirectionConstants(t *testing.T) {
	// Direction constants carry the exact DB-facing string values.
	if ledger.Debit != "DEBIT" {
		t.Fatalf("ledger.Debit = %q, want %q", ledger.Debit, "DEBIT")
	}
	if ledger.Credit != "CREDIT" {
		t.Fatalf("ledger.Credit = %q, want %q", ledger.Credit, "CREDIT")
	}

	txnID := uuid.New()
	e := ledger.Entry{
		TransactionID: txnID,
		Account:       "cash-clearing",
		Direction:     ledger.Debit,
		Amount:        money.Amount(15000), // RM150.00
	}
	if e.TransactionID != txnID {
		t.Fatalf("Entry.TransactionID = %s, want %s", e.TransactionID, txnID)
	}
	if e.Account != "cash-clearing" {
		t.Fatalf("Entry.Account = %q, want %q", e.Account, "cash-clearing")
	}
	if e.Direction != ledger.Debit {
		t.Fatalf("Entry.Direction = %q, want %q", e.Direction, ledger.Debit)
	}
	if e.Amount != money.Amount(15000) {
		t.Fatalf("Entry.Amount = %d, want 15000", int64(e.Amount))
	}
}

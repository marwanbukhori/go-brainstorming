// Package ledger holds the pure append-only double-entry ledger types and the
// per-transaction balanced-check invariant (ADR-002/003). No DB code lives here;
// persistence is in internal/store (PostEntries, FoldInvariant).
package ledger

import (
	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

// Direction is the side of a double-entry posting. The string values are the
// exact tokens stored in ledger_entries.direction (CHECK constraint, ADR-003).
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// Entry is one leg of a double-entry posting. Amount is ALWAYS > 0; the
// Direction carries the sign. There is no balance column anywhere — balances
// are always derived by folding entries (ADR-003).
type Entry struct {
	TransactionID uuid.UUID
	Account       string       // e.g. "cash-clearing", "fuel-revenue"
	Direction     Direction
	Amount        money.Amount // always > 0; Direction carries the sign
}

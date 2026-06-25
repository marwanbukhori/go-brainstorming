package ledger

import (
	"errors"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

// ErrUnbalanced is returned by Balanced when the entries do not form a valid
// balanced double-entry posting.
var ErrUnbalanced = errors.New("ledger entries not balanced: sum(debits) != sum(credits)")

// Balanced returns nil iff:
//   - the slice is non-empty,
//   - every Amount is strictly > 0 (Direction, not sign, carries the side), and
//   - sum of DEBIT amounts == sum of CREDIT amounts.
//
// Otherwise it returns ErrUnbalanced. This is the per-transaction double-entry
// invariant (ADR-002); the global invariant is the fold in store.FoldInvariant.
func Balanced(entries []Entry) error {
	if len(entries) == 0 {
		return ErrUnbalanced
	}
	var debits, credits money.Amount
	for _, e := range entries {
		if e.Amount <= 0 {
			return ErrUnbalanced
		}
		switch e.Direction {
		case Debit:
			debits = debits.Add(e.Amount)
		case Credit:
			credits = credits.Add(e.Amount)
		default:
			return ErrUnbalanced
		}
	}
	if debits != credits {
		return ErrUnbalanced
	}
	return nil
}

package ledger_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func leg(account string, dir ledger.Direction, amt int64) ledger.Entry {
	return ledger.Entry{
		TransactionID: uuid.New(),
		Account:       account,
		Direction:     dir,
		Amount:        money.Amount(amt),
	}
}

func TestBalanced(t *testing.T) {
	tests := []struct {
		name    string
		entries []ledger.Entry
		wantErr bool
	}{
		{
			name: "balanced one-to-one passes",
			entries: []ledger.Entry{
				leg("cash-clearing", ledger.Debit, 15000),
				leg("fuel-revenue", ledger.Credit, 15000),
			},
			wantErr: false,
		},
		{
			name: "balanced split debit passes",
			entries: []ledger.Entry{
				leg("cash-clearing", ledger.Debit, 500),
				leg("card-clearing", ledger.Debit, 2000),
				leg("fuel-revenue", ledger.Credit, 2500),
			},
			wantErr: false,
		},
		{
			name: "debit-only is rejected",
			entries: []ledger.Entry{
				leg("cash-clearing", ledger.Debit, 15000),
			},
			wantErr: true,
		},
		{
			name: "credit-only is rejected",
			entries: []ledger.Entry{
				leg("fuel-revenue", ledger.Credit, 15000),
			},
			wantErr: true,
		},
		{
			name:    "empty is rejected",
			entries: []ledger.Entry{},
			wantErr: true,
		},
		{
			name:    "nil is rejected",
			entries: nil,
			wantErr: true,
		},
		{
			name: "unequal sums rejected",
			entries: []ledger.Entry{
				leg("cash-clearing", ledger.Debit, 15000),
				leg("fuel-revenue", ledger.Credit, 14999),
			},
			wantErr: true,
		},
		{
			name: "zero amount rejected",
			entries: []ledger.Entry{
				leg("cash-clearing", ledger.Debit, 0),
				leg("fuel-revenue", ledger.Credit, 0),
			},
			wantErr: true,
		},
		{
			name: "negative amount rejected",
			entries: []ledger.Entry{
				leg("cash-clearing", ledger.Debit, -15000),
				leg("fuel-revenue", ledger.Credit, -15000),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ledger.Balanced(tc.entries)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Balanced(%s): want error, got nil", tc.name)
				}
				if !errors.Is(err, ledger.ErrUnbalanced) {
					t.Fatalf("Balanced(%s): want errors.Is ErrUnbalanced, got %v", tc.name, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Balanced(%s): want nil, got %v", tc.name, err)
			}
		})
	}
}

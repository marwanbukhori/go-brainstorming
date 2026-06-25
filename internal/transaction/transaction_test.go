package transaction

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func TestTransactionFields(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC()
	txn := Transaction{
		ID:             id,
		PumpID:         "pump-7",
		Status:         StatusAuthorizing,
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(15000),
		CapturedAmount: money.Amount(0),
		VolumeML:       0,
		AcquirerRef:    "",
		IdempotencyKey: "key-abc",
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if txn.ID != id {
		t.Errorf("ID = %v, want %v", txn.ID, id)
	}
	if txn.PumpID != "pump-7" {
		t.Errorf("PumpID = %q, want pump-7", txn.PumpID)
	}
	if txn.Status != StatusAuthorizing {
		t.Errorf("Status = %q, want AUTHORIZING", txn.Status)
	}
	if txn.AuthAmount != money.Amount(15000) {
		t.Errorf("AuthAmount = %d, want 15000", int64(txn.AuthAmount))
	}
	if txn.Version != 1 {
		t.Errorf("Version = %d, want 1", txn.Version)
	}
}

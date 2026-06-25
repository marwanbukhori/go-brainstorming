package transaction

import (
	"time"

	"github.com/google/uuid"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

// Transaction is the fuel-sale aggregate. It is persisted to the transactions
// table and mutated in place under optimistic concurrency (version-CAS).
type Transaction struct {
	ID             uuid.UUID
	PumpID         string
	Status         Status
	FuelGrade      string
	AuthAmount     money.Amount
	CapturedAmount money.Amount
	VolumeML       int64
	AcquirerRef    string
	IdempotencyKey string
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

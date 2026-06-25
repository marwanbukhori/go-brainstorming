package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/service"
)

// TestAuthorize_ConcurrentSameKey_EffectRunsOnce is a regression/safety-net
// test for ADR-001 atomic idempotency. The production guard (isUniqueViolation →
// resolveExisting) already lives in Authorize; this test proves that 16 goroutines
// firing the IDENTICAL request at once produce exactly one transaction row, one
// idempotency_keys row, and every caller returns nil error with the same id.
//
// If this test fails, the loser-resolution path in Authorize is broken — do NOT
// weaken the assertions; fix the production code.
func TestAuthorize_ConcurrentSameKey_EffectRunsOnce(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)

	req := service.AuthorizeRequest{
		IdempotencyKey: "pump-7-authorize-concurrent",
		RequestHash:    "hash-v1",
		PumpID:         "pump-7",
		FuelGrade:      "RON97",
		AuthAmount:     money.Amount(20000), // RM200.00
	}

	const goroutines = 16

	ids := make([]uuid.UUID, goroutines)
	errs := make([]error, goroutines)

	// start is held at 1 until all goroutines are ready, then released at once
	// to maximise the chance of hitting the COMMIT-race path.
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	done.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer done.Done()
			start.Wait() // barrier: all goroutines launch together
			ids[i], errs[i] = a.Authorize(ctx, req)
		}(i)
	}

	start.Done() // fire all goroutines at once
	done.Wait()

	// DETERMINISTIC EXPECTATION 1: every caller returns nil error — the loser of
	// the unique-insert race resolves the stored id; it never surfaces as an error.
	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d errored: %v", i, errs[i])
		}
	}

	// DETERMINISTIC EXPECTATION 2: all 16 ids are identical — one transaction id.
	want := ids[0]
	for i := 1; i < goroutines; i++ {
		if ids[i] != want {
			t.Fatalf("goroutine %d returned id %s, want the single id %s", i, ids[i], want)
		}
	}

	// DETERMINISTIC EXPECTATION 3: the effect executed exactly once.
	var txns int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE pump_id = $1`, req.PumpID).Scan(&txns); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if txns != 1 {
		t.Fatalf("want exactly 1 transaction row after %d concurrent identical requests, got %d", goroutines, txns)
	}

	// DETERMINISTIC EXPECTATION 4: exactly one idempotency_keys row.
	var keys int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, req.IdempotencyKey).Scan(&keys); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if keys != 1 {
		t.Fatalf("want exactly 1 idempotency_keys row, got %d", keys)
	}
}

# Fuel POS — Phase 0–3 (Local Money Core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the local, zero-cost money core of the fuel-POS platform — the transaction state machine with optimistic concurrency, atomic idempotency, and an append-only double-entry ledger with a continuously-verifiable invariant — all proven by tests against a real Postgres under `-race`.

**Architecture:** A1 synchronous money core (one Postgres ACID boundary). Every money-defining act — state transition + idempotency claim + ledger post — commits in a single transaction. Optimistic concurrency via a `version` column (compare-and-swap). Idempotency via a `UNIQUE` key claimed in the *same* transaction as its effect (ADR-001). Append-only ledger with no balance row; the global invariant `Σ debits = Σ credits` is expressed as a fold/SUM so it survives a future shard-by-pump migration (ADR-002/003/010). This plan is Phases 0–3 of the 18-phase roadmap in the platform spec; it deliberately stops before the bus, sharding, `transaction_events`, and the acquirer saga (later plans, ADR-007 lean footprint).

**Tech Stack:** Go 1.22+ · Postgres 16 (Docker Compose) · `jackc/pgx/v5` + `pgxpool` · `golang-migrate/v4` · `google/uuid` · `testcontainers-go` · stdlib `testing` run with `-race` · `log/slog`.

**Source spec:** `docs/superpowers/specs/2026-06-25-fuel-pos-platform-design.md` (Roadmap Phases 0–3; v1 core at `files/ARCHITECTURE.md`).

## Global Constraints

*Every task's requirements implicitly include this section.*

- **Integer minor units only** (sen); no floating point anywhere near money (`money.Amount = int64`).
- **MYR-only** — no `currency` column or dimension (ADR-016).
- **The money path is synchronous, inside ONE Postgres transaction.** Nothing money-defining touches a bus (no bus exists in this plan).
- **Idempotency key is claimed in the SAME transaction as its effect** — a crash can never persist the key without the effect (ADR-001).
- **Ledger is append-only; no balance column** — balances are derived (ADR-003). Per-transaction `Σ debits = Σ credits` is checked in pure Go *before* any write; the global invariant is a fold/SUM over the ledger (ADR-002/010).
- **`captured_amount ≤ auth_amount` is a hard guard**, enforced at the service layer in the capture transaction (spec §8) → `store.ErrCaptureExceedsAuth`.
- **One live transaction per pump** via the `one_active_txn_per_pump` partial unique index, not a lock service (ADR-004).
- **The version-CAS SQL (`WHERE id=$id AND version=$expected`) and the transaction INSERT each exist in exactly ONE place** — the tx-scoped cores `ApplyTransitionTx` / `CreateTransactionTx`; ctx-only methods are thin BEGIN/COMMIT wrappers. Never copy-paste the safety-critical SQL.
- **Module path:** `github.com/marwanbukhori/go-brainstorming`.
- **Tests:** TDD throughout (failing test → run/fail → minimal impl → run/pass → commit). All tests run with `-race`. Integration tests hit a **real Postgres** via testcontainers (the invariants are DB-enforced) and `t.Skip` when Docker is absent; unit tests (money, state machine, `Balanced`) are pure.
- **Commits:** Conventional Commits (`feat:`/`test:`/`chore:`). Every commit body ends with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Out of scope for this plan** (later plans): the event bus/outbox, sharding + cross-shard clearing, `transaction_events`, the acquirer mock + saga, the expiry reaper, the CQRS read-side, POS, reporting, K8s/AWS.

---

## Shared Contracts — Canonical Names, Types & DDL

**These are the canonical names, types, signatures, DDL, and conventions. Every task must use these EXACTLY. Do not invent alternative names.** This realises the v1 money core + the spec's ADR-001..004 and ADR-010(fold framing) locally, with zero cloud spend.

### Tech stack (pinned)

- **Go 1.22+** · module path `github.com/marwanbukhori/go-brainstorming`
- **Postgres 16** via Docker Compose (local only)
- **DB driver:** `github.com/jackc/pgx/v5` + `pgxpool`
- **Migrations:** `github.com/golang-migrate/migrate/v4` (file source, embedded), SQL files under `internal/store/migrations/`
- **UUID:** `github.com/google/uuid`
- **Tests:** Go stdlib `testing`, always run with `-race`. Integration tests use **`github.com/testcontainers/testcontainers-go`** + its `modules/postgres` to spin a real Postgres (the invariants are DB-enforced, so integration tests hit real Postgres, never a mock). Unit tests (state machine, money, balanced-check) are pure and need no DB.
- **Logging:** stdlib `log/slog`.
- No other dependencies in Phases 0–3 (ADR-007 lean footprint).

### Repository layout (created across Phases 0–3)

```
go.mod / go.sum
Makefile
docker-compose.yml                  # postgres:16
.env.example                        # DATABASE_URL=postgres://fuelpos:fuelpos@localhost:5432/fuelpos?sslmode=disable
cmd/fuelpos/main.go                 # entrypoint: connect pool, run migrations, log ready (grows in later plans)
internal/
  money/
    amount.go                       # Amount minor-units type
    amount_test.go
  transaction/
    status.go                       # Status enum + IsTerminal
    event.go                        # Event enum
    statemachine.go                 # Apply(from,event) transition table
    statemachine_test.go
    transaction.go                  # Transaction aggregate struct
  ledger/
    entry.go                        # Direction + Entry
    posting.go                      # Balanced(entries)
    posting_test.go
  store/
    postgres.go                     # Store wraps *pgxpool.Pool; Migrate()
    transactions.go                 # CreateTransaction, GetTransaction, ApplyTransition (version-CAS)
    idempotency.go                  # ClaimIdempotencyKey (same-txn)
    ledger.go                       # PostEntries (same-txn), FoldInvariant
    transactions_test.go            # testcontainers integration
    idempotency_test.go             # testcontainers integration
    ledger_test.go                  # testcontainers integration
    migrations/
      0001_transactions.up.sql / 0001_transactions.down.sql
      0002_idempotency_keys.up.sql / 0002_idempotency_keys.down.sql
      0003_ledger_entries.up.sql / 0003_ledger_entries.down.sql
```

### Canonical Go contracts

#### `internal/money`
```go
package money

// Amount is money in integer minor units (sen). 100 = RM1.00. No floats, ever (spec §8 / ADR money rules).
type Amount int64

func (a Amount) Add(b Amount) Amount  // a + b
func (a Amount) Sub(b Amount) Amount  // a - b
func (a Amount) Neg() Amount          // -a
func (a Amount) IsZero() bool         // a == 0
func (a Amount) String() string       // "RM12.34" (sign-aware)
```

#### `internal/transaction`
```go
package transaction

type Status string
const (
    StatusAuthorizing Status = "AUTHORIZING"
    StatusAuthorized  Status = "AUTHORIZED"
    StatusDispensing  Status = "DISPENSING"
    StatusCompleted   Status = "COMPLETED"
    StatusCapturing   Status = "CAPTURING"
    StatusCaptured    Status = "CAPTURED"
    StatusSettled     Status = "SETTLED"
    StatusDeclined    Status = "DECLINED"
    StatusVoided      Status = "VOIDED"
    StatusExpired     Status = "EXPIRED"
    StatusReversed    Status = "REVERSED"
    StatusFailed      Status = "FAILED"
)
// IsTerminal reports whether the status is one of the terminal set
// (SETTLED, DECLINED, VOIDED, EXPIRED, REVERSED, FAILED). Used by the
// one_active_txn_per_pump partial index predicate (ADR-004).
func (s Status) IsTerminal() bool

type Event string
const (
    EventAuthorize        Event = "Authorize"
    EventAcquirerApproved Event = "AcquirerApproved"
    EventAcquirerDeclined Event = "AcquirerDeclined"
    EventStartDispense    Event = "StartDispense"
    EventPumpStopped      Event = "PumpStopped"
    EventCapture          Event = "Capture"
    EventAcquirerCaptured Event = "AcquirerCaptured"
    EventHoldTimeout      Event = "HoldTimeout"
    EventCancel           Event = "Cancel"
    EventReverse          Event = "Reverse"
    EventIncludeInBatch   Event = "IncludeInBatch"
)

var ErrIllegalTransition = errors.New("illegal transition")

// Apply returns the destination status for a legal (from,event) pair,
// or ErrIllegalTransition. Pure function over the transition table; amount
// guards (e.g. captured<=auth) are enforced at the store/service layer, not here.
func Apply(from Status, e Event) (Status, error)

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
```

Legal transitions Apply() must encode (from v1 doc §4):
| From | Event | To |
|---|---|---|
| (zero Status "") | Authorize | AUTHORIZING |
| AUTHORIZING | AcquirerApproved | AUTHORIZED |
| AUTHORIZING | AcquirerDeclined | DECLINED |
| AUTHORIZED | StartDispense | DISPENSING |
| AUTHORIZED | HoldTimeout | EXPIRED |
| AUTHORIZED | Cancel | VOIDED |
| DISPENSING | PumpStopped | COMPLETED |
| COMPLETED | Capture | CAPTURING |
| CAPTURING | AcquirerCaptured | CAPTURED |
| CAPTURED | IncludeInBatch | SETTLED |
| CAPTURED | Reverse | REVERSED |

#### `internal/ledger`
```go
package ledger

type Direction string
const (
    Debit  Direction = "DEBIT"
    Credit Direction = "CREDIT"
)

type Entry struct {
    TransactionID uuid.UUID
    Account       string       // e.g. "cash-clearing", "fuel-revenue"
    Direction     Direction
    Amount        money.Amount // always > 0; Direction carries the sign
}

var ErrUnbalanced = errors.New("ledger entries not balanced: sum(debits) != sum(credits)")

// Balanced returns nil iff sum of DEBIT amounts == sum of CREDIT amounts and
// the slice is non-empty with all amounts > 0; otherwise ErrUnbalanced (or a
// validation error). This is the per-transaction double-entry invariant (ADR-002).
func Balanced(entries []Entry) error
```

#### `internal/store`
```go
package store

type Store struct { Pool *pgxpool.Pool }

func New(ctx context.Context, dsn string) (*Store, error) // opens pgxpool
func (s *Store) Close()
func (s *Store) Migrate() error                            // runs golang-migrate up to latest

var ErrVersionConflict = errors.New("optimistic version conflict")
var ErrNotFound        = errors.New("transaction not found")

// --- Phase 1: transactions ---
// There is ONE insert implementation and ONE version-CAS implementation. The
// ctx-only methods are thin wrappers that BEGIN/COMMIT their own tx around the
// tx-scoped core; callers that must share a transaction (e.g. the idempotency
// service in Phase 2, the capture path in Phase 3) call the *Tx core directly so
// the safety-critical SQL is never copy-pasted.

// CreateTransactionTx inserts t (all columns, incl. t.IdempotencyKey -> idempotency_key)
// at Version 1 using the caller's tx. The ctx-only CreateTransaction wraps it.
func (s *Store) CreateTransactionTx(ctx context.Context, tx pgx.Tx, t *transaction.Transaction) error
func (s *Store) CreateTransaction(ctx context.Context, t *transaction.Transaction) error

func (s *Store) GetTransaction(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)

// ApplyTransitionTx is the version-CAS core: it loads the row in the caller's tx,
// computes the next status via transaction.Apply, lets mutate() adjust amount/acquirer
// fields, then does
//   UPDATE transactions SET status=$next, version=version+1, ... WHERE id=$id AND version=$expectedVersion
// returning ErrVersionConflict if 0 rows were affected (a concurrent writer won),
// ErrNotFound if the row is absent. It loads/returns ALL columns incl. CreatedAt/UpdatedAt.
// Not-found checks use errors.Is(err, pgx.ErrNoRows). The ctx-only ApplyTransition wraps it.
func (s *Store) ApplyTransitionTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, expectedVersion int64, e transaction.Event, mutate func(*transaction.Transaction)) (*transaction.Transaction, error)
func (s *Store) ApplyTransition(ctx context.Context, id uuid.UUID, expectedVersion int64, e transaction.Event, mutate func(*transaction.Transaction)) (*transaction.Transaction, error)

// --- Phase 2: idempotency (claim must be in the SAME tx as the effect, ADR-001) ---
type IdempotencySnapshot struct {
    TransactionID uuid.UUID
    ResponseJSON  []byte
}
// ClaimIdempotencyKey attempts INSERT ... ON CONFLICT (key) DO NOTHING within tx.
// If the row was inserted, returns (nil, true, nil) — caller proceeds with the effect.
// If the key already existed, returns (existing snapshot, false, nil) — caller returns the stored result, no re-execution.
func (s *Store) ClaimIdempotencyKey(ctx context.Context, tx pgx.Tx, key, requestHash string, txnID uuid.UUID) (existing *IdempotencySnapshot, claimed bool, err error)

// --- Phase 3: ledger (append-only; posting in the SAME tx as the state change, ADR-002/003) ---
// PostEntries calls ledger.Balanced(entries) first (returns ErrUnbalanced on failure),
// then bulk-INSERTs the entries. Append-only: never updates/deletes.
func (s *Store) PostEntries(ctx context.Context, tx pgx.Tx, entries []ledger.Entry) error
// FoldInvariant returns the global sums; the continuous invariant is debits == credits (ADR-002).
// Expressed as a fold/SUM so it survives the future shard-by-pump migration (ADR-010 framing).
func (s *Store) FoldInvariant(ctx context.Context) (debits money.Amount, credits money.Amount, err error)

// --- Phase 3: capture money guard (spec §8 / ADR money rules) ---
// captured_amount <= auth_amount is a HARD guard, enforced at the service layer in the
// capture transaction (NOT in the pure state machine). Violations return ErrCaptureExceedsAuth.
var ErrCaptureExceedsAuth = errors.New("captured amount exceeds authorized amount")
```

### Canonical DDL

`0001_transactions.up.sql`:
```sql
CREATE TABLE transactions (
    id                    uuid        PRIMARY KEY,
    pump_id               text        NOT NULL,
    status                text        NOT NULL,
    fuel_grade            text        NOT NULL DEFAULT '',
    auth_amount_minor     bigint      NOT NULL DEFAULT 0,
    captured_amount_minor bigint      NOT NULL DEFAULT 0,
    volume_ml             bigint      NOT NULL DEFAULT 0,
    acquirer_ref          text        NOT NULL DEFAULT '',
    idempotency_key       text        NOT NULL,
    version               bigint      NOT NULL DEFAULT 1,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);
-- ADR-004: one live transaction per pump, enforced by a partial unique index (not a lock service)
CREATE UNIQUE INDEX one_active_txn_per_pump ON transactions (pump_id)
    WHERE status NOT IN ('SETTLED','DECLINED','VOIDED','EXPIRED','REVERSED','FAILED');
```

`0002_idempotency_keys.up.sql`:
```sql
CREATE TABLE idempotency_keys (
    key               text        PRIMARY KEY,           -- the UNIQUE constraint IS the dedupe mechanism (ADR-001)
    request_hash      text        NOT NULL,
    transaction_id    uuid        NOT NULL,
    response_snapshot jsonb,
    created_at        timestamptz NOT NULL DEFAULT now()
);
```

`0003_ledger_entries.up.sql`:
```sql
CREATE TABLE ledger_entries (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id uuid        NOT NULL,
    account        text        NOT NULL,
    direction      text        NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),  -- ADR-003: no balance column; balances derived
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_txn_idx ON ledger_entries (transaction_id);
-- Append-only by convention (no UPDATE/DELETE in code). A revoke + trigger guard arrives in a later plan.
```
Each `*.down.sql` drops the corresponding table (and indexes drop with it).

### Test & commit conventions

- **TDD always:** write the failing test → run it, see it fail → minimal implementation → run, see it pass → commit. One behaviour per test.
- **Run unit tests:** `go test -race ./internal/money/... ./internal/transaction/... ./internal/ledger/...`
- **Run integration tests:** `go test -race ./internal/store/...` (testcontainers pulls postgres:16; requires Docker running).
- **Commit messages:** Conventional Commits (`feat:`, `test:`, `chore:`). End every commit body with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- **Concurrency proof (the money lessons):** Phase 1 includes an integration test that fires two concurrent conflicting `ApplyTransition` calls on the same row and asserts exactly one succeeds and the other gets `ErrVersionConflict`. Phase 2 includes a test that two concurrent identical requests with the same idempotency key execute the effect once. Phase 3 includes a test that `PostEntries` rejects unbalanced entries and that `FoldInvariant` returns `debits == credits` after a batch of balanced postings.

---

## Phase 0 — Project Skeleton & Money Type

Stand up the local-only foundation for the fuel-POS money core: initialise the Go module `github.com/marwanbukhori/go-brainstorming` with the pinned dependencies (pgx/v5, golang-migrate/v4, google/uuid, testcontainers-go), wire the zero-cloud-spend developer environment (Docker Compose `postgres:16`, `.env.example`, Makefile), build the float-free `money.Amount` minor-units type under full TDD, and wire `internal/store.Store` (pool + embedded-FS `Migrate()`) plus the `cmd/fuelpos` entrypoint. The `money` tasks (3, 4) are genuine TDD: a pure unit test goes red, then green. The infra/skeleton tasks (1, 2, 5, 7) are NOT TDD — their first step is a build/wiring gate, not a real unit-test red phase, and is labelled honestly as such. A single `testcontainers` smoke test (Task 6) proves `New` + `Pool.Ping` against a real Postgres. NOTE: `Migrate()` is wired in this phase but the first migration file (`0001`) is not created until Phase 1, so the Phase-0 smoke test exercises `New` + `Ping` ONLY and must NOT call `Migrate()` (golang-migrate errors with "no migration found" against an empty FS).

### Task 1: Go Module Init & Dependencies

**Files:**
- Create: `go.mod`
- Create: `go.sum` (generated by `go mod tidy`)
- Create: `.gitignore` (already present at repo root — verify, do not clobber)

**Interfaces:**
- Consumes: nothing (first task)
- Produces: module `github.com/marwanbukhori/go-brainstorming` on the import path; deps `github.com/jackc/pgx/v5`, `github.com/golang-migrate/migrate/v4`, `github.com/google/uuid`, `github.com/testcontainers/testcontainers-go` + `modules/postgres` resolvable by all later tasks.

> This task is project bootstrap, not TDD. There is no behaviour to drive out with a unit test; Steps 1–2 are an infra gate that proves the module does not yet exist, and Step 4 confirms it now does. Do not expect a real unit-test failure here.

- [ ] **Step 1: Verification (infra gate, not a TDD red phase)**

There is no Go code yet, so this is a build/wiring gate, not a TDD red phase: prove the module does not yet exist.
```bash
test ! -f go.mod && echo "NO_GO_MOD_YET" || echo "GO_MOD_EXISTS"
```
Expected output: `NO_GO_MOD_YET`

- [ ] **Step 2: Run the gate, confirm the module is absent**

Run: `go list -m 2>&1 || echo "EXIT=$?"`
Expected: gate fails with `go: cannot find main module; see 'go help modules'` (no module yet). This is the expected pre-bootstrap state, not a TDD red.

- [ ] **Step 3: Implement**
```bash
# from repo root /Users/marwanbukhori/conductor/workspaces/go-brainstorming/windhoek
go mod init github.com/marwanbukhori/go-brainstorming

# pinned deps (ADR-007 lean footprint: only these in Phases 0–3)
go get github.com/jackc/pgx/v5@latest
go get github.com/golang-migrate/migrate/v4@latest
go get github.com/google/uuid@latest
go get github.com/testcontainers/testcontainers-go@latest
go get github.com/testcontainers/testcontainers-go/modules/postgres@latest

go mod tidy
```
Confirm `go.mod` declares `go 1.22` or higher (edit the `go` directive to `go 1.22` if the local toolchain wrote a newer patch line and you want the floor pinned):
```
module github.com/marwanbukhori/go-brainstorming

go 1.22
```

- [ ] **Step 4: Run the gate, confirm it passes**

Run: `go list -m && go build ./... 2>&1 | grep -v "no Go files" || true`
Expected: PASS — prints `github.com/marwanbukhori/go-brainstorming`; `go build ./...` succeeds (no packages yet is fine).

- [ ] **Step 5: Commit**
```bash
git add go.mod go.sum
git commit -m "chore: init go module github.com/marwanbukhori/go-brainstorming with pinned deps

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Local Dev Environment (Compose, .env.example, Makefile)

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `Makefile`

**Interfaces:**
- Consumes: nothing from Go code (infrastructure only)
- Produces: a `postgres:16` service reachable at `localhost:5432` with db/user/pass = `fuelpos`; canonical `DATABASE_URL` in `.env.example`; Makefile targets `up`, `down`, `test`, `test-race`, `migrate`, `run` that later tasks/phases invoke.

> This task is infrastructure config, not TDD. There is no Go behaviour under test; Steps 1–2 are an infra gate proving the env files are absent, and Step 4 validates the config once written. Do not expect a real unit-test failure here.

- [ ] **Step 1: Verification (infra gate, not a TDD red phase)**

This is a config/wiring gate, not a TDD red phase: prove the compose config and canonical DSN do not exist yet.
```bash
test ! -f docker-compose.yml && test ! -f .env.example && echo "ENV_MISSING" || echo "ENV_PRESENT"
```
Expected output: `ENV_MISSING`

- [ ] **Step 2: Run the gate, confirm the config is absent**

Run: `docker compose config 2>&1 || echo "EXIT=$?"`
Expected: gate fails with `no configuration file provided: not found` (no `docker-compose.yml`). Expected pre-config state, not a TDD red.

- [ ] **Step 3: Implement**

`docker-compose.yml`:
```yaml
services:
  postgres:
    image: postgres:16
    container_name: fuelpos-postgres
    environment:
      POSTGRES_DB: fuelpos
      POSTGRES_USER: fuelpos
      POSTGRES_PASSWORD: fuelpos
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U fuelpos -d fuelpos"]
      interval: 2s
      timeout: 3s
      retries: 15
    volumes:
      - fuelpos_pgdata:/var/lib/postgresql/data

volumes:
  fuelpos_pgdata:
```

`.env.example`:
```
DATABASE_URL=postgres://fuelpos:fuelpos@localhost:5432/fuelpos?sslmode=disable
```

`Makefile` (uses TAB indentation for recipe lines):
```makefile
.PHONY: up down test test-race migrate run

up:
	docker compose up -d

down:
	docker compose down

test:
	go test ./...

test-race:
	go test -race ./...

# Runs migrations against DATABASE_URL by booting the entrypoint, which calls store.Migrate().
migrate:
	go run ./cmd/fuelpos

run:
	go run ./cmd/fuelpos
```

- [ ] **Step 4: Run the gate, confirm it passes**
```bash
docker compose config >/dev/null && echo "COMPOSE_OK"
grep -q 'postgres://fuelpos:fuelpos@localhost:5432/fuelpos?sslmode=disable' .env.example && echo "DSN_OK"
make -n up down test test-race migrate run >/dev/null && echo "MAKE_OK"
```
Expected: PASS — prints `COMPOSE_OK`, `DSN_OK`, `MAKE_OK`.

- [ ] **Step 5: Commit**
```bash
git add docker-compose.yml .env.example Makefile
git commit -m "chore: local dev env (compose postgres:16, .env.example, Makefile)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Money Amount — Add, Sub, Neg, IsZero

**Files:**
- Create: `internal/money/amount.go`
- Test: `internal/money/amount_test.go`

**Interfaces:**
- Consumes: nothing (pure type)
- Produces:
  - `type Amount int64`
  - `func (a Amount) Add(b Amount) Amount`
  - `func (a Amount) Sub(b Amount) Amount`
  - `func (a Amount) Neg() Amount`
  - `func (a Amount) IsZero() bool`

  Later tasks/phases rely on these (`transaction.AuthAmount`, `ledger.Entry.Amount`).

> This is real TDD: a pure unit test drives out a real type. The test genuinely goes red (fails to compile) before the implementation lands, then goes green.

- [ ] **Step 1: Write the failing test**

`internal/money/amount_test.go`:
```go
package money

import "testing"

func TestAdd(t *testing.T) {
	cases := []struct {
		name     string
		a, b, want Amount
	}{
		{"positives", 100, 25, 125},
		{"add zero", 100, 0, 100},
		{"negative result", 100, -250, -150},
		{"two negatives", -100, -25, -125},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.Add(c.b); got != c.want {
				t.Fatalf("%d.Add(%d) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestSub(t *testing.T) {
	cases := []struct {
		name       string
		a, b, want Amount
	}{
		{"positives", 100, 25, 75},
		{"sub zero", 100, 0, 100},
		{"goes negative", 25, 100, -75},
		{"double negative", -100, -25, -75},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.Sub(c.b); got != c.want {
				t.Fatalf("%d.Sub(%d) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestNeg(t *testing.T) {
	cases := []struct {
		in, want Amount
	}{
		{100, -100},
		{-100, 100},
		{0, 0},
	}
	for _, c := range cases {
		if got := c.in.Neg(); got != c.want {
			t.Fatalf("%d.Neg() = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestIsZero(t *testing.T) {
	if !Amount(0).IsZero() {
		t.Fatal("Amount(0).IsZero() = false, want true")
	}
	if Amount(1).IsZero() {
		t.Fatal("Amount(1).IsZero() = true, want false")
	}
	if Amount(-1).IsZero() {
		t.Fatal("Amount(-1).IsZero() = true, want false")
	}
}
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/money/... -run 'TestAdd|TestSub|TestNeg|TestIsZero' -v`
Expected: FAIL — does not compile: `undefined: Amount` (no `amount.go` yet) / `package money: no Go files`.

- [ ] **Step 3: Implement**

`internal/money/amount.go`:
```go
// Package money holds the float-free monetary type for the fuel-POS core.
// Amount is integer minor units (sen); 100 == RM1.00. No floats, ever
// (spec §8 / money rules).
package money

// Amount is money in integer minor units (sen). 100 = RM1.00.
type Amount int64

// Add returns a + b.
func (a Amount) Add(b Amount) Amount { return a + b }

// Sub returns a - b.
func (a Amount) Sub(b Amount) Amount { return a - b }

// Neg returns -a.
func (a Amount) Neg() Amount { return -a }

// IsZero reports whether a == 0.
func (a Amount) IsZero() bool { return a == 0 }
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/money/... -run 'TestAdd|TestSub|TestNeg|TestIsZero' -v`
Expected: PASS — `ok  github.com/marwanbukhori/go-brainstorming/internal/money`.

- [ ] **Step 5: Commit**
```bash
git add internal/money/amount.go internal/money/amount_test.go
git commit -m "feat: money.Amount minor-units type with Add/Sub/Neg/IsZero

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Money Amount — String (sign-aware, sub-RM1 formatting)

**Files:**
- Modify: `internal/money/amount.go` (append `String()` method)
- Test: `internal/money/amount_test.go` (append `TestString`)

**Interfaces:**
- Consumes: `type Amount int64` (Task 3)
- Produces: `func (a Amount) String() string` — `"RM12.34"`, sign-aware (e.g. `"-RM1.50"`), correct sub-RM1 zero-padding (e.g. `"-RM0.05"`, `"RM0.09"`).

> This is real TDD: `TestString` goes red (fails to compile against the missing method) before `String()` is implemented, then goes green.

- [ ] **Step 1: Write the failing test**

Append to `internal/money/amount_test.go`:
```go
func TestString(t *testing.T) {
	cases := []struct {
		in   Amount
		want string
	}{
		{1234, "RM12.34"},
		{100, "RM1.00"},
		{0, "RM0.00"},
		{9, "RM0.09"},
		{99, "RM0.99"},
		{5, "RM0.05"},
		{-5, "-RM0.05"},
		{-150, "-RM1.50"},
		{-1234, "-RM12.34"},
		{-9, "-RM0.09"},
		{1000000, "RM10000.00"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Fatalf("Amount(%d).String() = %q, want %q", int64(c.in), got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/money/... -run TestString -v`
Expected: FAIL — does not compile: `c.in.String undefined (type Amount has no field or method String)`.

- [ ] **Step 3: Implement**

Append to `internal/money/amount.go`:
```go
import (
	"fmt"
)

// String renders the amount as ringgit with two decimal places, sign-aware.
// Sub-RM1 magnitudes are zero-padded ("RM0.05"); negatives keep the sign
// outside the currency prefix ("-RM0.05", "-RM12.34").
func (a Amount) String() string {
	n := int64(a)
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	return fmt.Sprintf("%sRM%d.%02d", sign, n/100, n%100)
}
```
NOTE: place the `import` block at the top of `amount.go`, immediately after the `package money` line (merge with any existing imports rather than declaring a second `import` block).

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/money/... -v`
Expected: PASS — `ok  github.com/marwanbukhori/go-brainstorming/internal/money`; all of `TestAdd`, `TestSub`, `TestNeg`, `TestIsZero`, `TestString` pass.

- [ ] **Step 5: Commit**
```bash
git add internal/money/amount.go internal/money/amount_test.go
git commit -m "feat: money.Amount String() sign-aware sub-RM1 formatting

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Store skeleton — New, Close, Migrate (embedded migrations FS)

**Files:**
- Create: `internal/store/postgres.go`
- Create: `internal/store/migrations/.keep` (empty placeholder so `embed` has a non-empty directory; Phase 1 replaces it with `0001_*.sql`)

**Interfaces:**
- Consumes: `*pgxpool.Pool` (`github.com/jackc/pgx/v5/pgxpool`); golang-migrate iofs + pgx drivers.
- Produces:
  - `type Store struct { Pool *pgxpool.Pool }`
  - `func New(ctx context.Context, dsn string) (*Store, error)`
  - `func (s *Store) Close()`
  - `func (s *Store) Migrate() error`
  - `var ErrVersionConflict = errors.New("optimistic version conflict")`
  - `var ErrNotFound = errors.New("transaction not found")`

  Phase 1 (`transactions.go`) consumes `Store.Pool` and these sentinels.

> This task is wiring, not TDD. Behaviour is proven by the Task 6 integration smoke test, not a unit test here; Steps 1–2 are a compile/wiring gate proving the package does not yet build, and Step 4 confirms it compiles. Do not expect a real unit-test failure here.

- [ ] **Step 1: Verification (infra gate, not a TDD red phase)**

This task has no standalone unit test (it is exercised by the Task 6 smoke test). The gate is a compile/vet check that the package does not yet build — a wiring gate, not a TDD red phase:

Run: `go build ./internal/store/... 2>&1 || echo "EXIT=$?"`
Expected: gate fails with `package ... internal/store: no Go files` (no `postgres.go` yet).

- [ ] **Step 2: Run the gate, confirm the package is absent**

Run: `go vet ./internal/store/... 2>&1 || echo "EXIT=$?"`
Expected: gate fails — `no Go files in .../internal/store`. Expected pre-wiring state, not a TDD red.

- [ ] **Step 3: Implement**

Create the placeholder so the embedded FS is valid before any SQL exists:
```bash
mkdir -p internal/store/migrations
touch internal/store/migrations/.keep
```

`internal/store/postgres.go`:
```go
// Package store wraps the Postgres connection pool and DB-enforced invariants
// for the fuel-POS money core. All money invariants (one-active-txn-per-pump,
// idempotency-key uniqueness, double-entry balance) are enforced in Postgres,
// so integration tests run against a real database, never a mock.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store wraps the pgx connection pool.
type Store struct {
	Pool *pgxpool.Pool
}

// ErrVersionConflict is returned when an optimistic version-CAS update affects
// zero rows because a concurrent writer won.
var ErrVersionConflict = errors.New("optimistic version conflict")

// ErrNotFound is returned when a transaction row does not exist.
var ErrNotFound = errors.New("transaction not found")

// New opens a pgx connection pool against dsn and verifies connectivity.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{Pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() {
	if s.Pool != nil {
		s.Pool.Close()
	}
}

// Migrate runs all up migrations embedded under migrations/ to the latest
// version using golang-migrate over an iofs source and the pgx/v5 driver.
//
// NOTE (Phase 0): the migrations directory contains no SQL files yet — the
// first migration (0001_transactions) lands in Phase 1. Do not call Migrate()
// before a migration exists; golang-migrate returns "no migration found" /
// ErrNilVersion against an empty source.
func (s *Store) Migrate() error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("store: migrate source: %w", err)
	}
	driver, err := pgxv5.WithInstance(stdlibFromPool(s.Pool), &pgxv5.Config{})
	if err != nil {
		return fmt.Errorf("store: migrate driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("store: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate up: %w", err)
	}
	return nil
}
```

The golang-migrate pgx/v5 database driver needs a `*sql.DB`, not a `*pgxpool.Pool`. Add the bridge helper in the same file (uses the pgx stdlib adapter against the same DSN config the pool already parsed):
```go
import (
	"database/sql"

	"github.com/jackc/pgx/v5/stdlib"
)

// stdlibFromPool opens a database/sql DB from the pool's connection config so
// the golang-migrate pgx/v5 driver (which requires *sql.DB) can run against the
// same target database the pool is connected to.
func stdlibFromPool(pool *pgxpool.Pool) *sql.DB {
	cfg := pool.Config().ConnConfig
	return stdlib.OpenDB(*cfg)
}
```
NOTE: merge all `import` lines into the single import block at the top of `postgres.go` (do not declare three separate `import` blocks). The migrate pgx driver is imported as `pgxv5` via the alias `pgxv5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"`; update the import line to:
```go
	pgxv5 "github.com/golang-migrate/migrate/v4/database/pgx/v5"
```

- [ ] **Step 4: Run the gate, confirm it passes**

Run: `go build ./internal/store/... && go vet ./internal/store/...`
Expected: PASS — both succeed with no output (package compiles; `migrationsFS` embeds the directory containing `.keep`).

- [ ] **Step 5: Commit**
```bash
git add internal/store/postgres.go internal/store/migrations/.keep
git commit -m "feat: store.Store with pgxpool New/Close and embedded-FS Migrate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Integration SMOKE test — testcontainers New + Pool.Ping

**Files:**
- Test: `internal/store/postgres_test.go`

**Interfaces:**
- Consumes: `store.New(ctx, dsn) (*Store, error)`, `(*Store).Close()`, `Store.Pool` (Task 5); `github.com/testcontainers/testcontainers-go/modules/postgres`.
- Produces: nothing consumed downstream — proves the wiring boots against a real Postgres.

> This is an INFRA SMOKE TEST, not a unit-level TDD cycle. It exercises `New` + `Pool.Ping` ONLY against a real `postgres:16` container — it does NOT call `Migrate()`, because the first migration file (`0001_transactions`) is not created until Phase 1; running `Migrate()` now against the empty migrations FS would fail. The test has no independent red phase of its own beyond the missing `New`/`Store` symbols from Task 5; once Task 5 is done it simply goes green. Phase 1's integration tests start calling `Migrate()` once `0001` exists.

- [ ] **Step 1: Write the smoke test**

`internal/store/postgres_test.go`:
```go
package store

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres spins a real postgres:16 container and returns its DSN.
// Shared by all store integration tests.
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx,
		"postgres:16",
		tcpostgres.WithDatabase("fuelpos"),
		tcpostgres.WithUsername("fuelpos"),
		tcpostgres.WithPassword("fuelpos"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pg.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

func TestStoreNewAndPing(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(t)

	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if s.Pool == nil {
		t.Fatal("Store.Pool is nil after New")
	}
	if err := s.Pool.Ping(ctx); err != nil {
		t.Fatalf("Pool.Ping: %v", err)
	}
}
```

- [ ] **Step 2: Run the smoke test against the Task 5 wiring**

This is an infra smoke test, not a TDD red phase: it has no independent failing state beyond the symbols Task 5 provides. If Task 5's `New`/`Close`/`Store` are not yet present, it fails to compile (`undefined: New`, `undefined: Store`); once Task 5 is done it goes straight to the Step 4 PASS.

Run: `go test -race ./internal/store/... -run TestStoreNewAndPing -v`
Expected (only if Task 5 has not yet landed): FAIL to compile — `undefined: New`, `undefined: Store`. With Task 5 complete, proceed directly to Step 4's PASS.

- [ ] **Step 3: Implement**

No new production code: Task 5 already provides `New`, `Close`, and `Store.Pool`, and this smoke test calls only `New` + `Pool.Ping` (no `Migrate()`). The only requirement is Docker running locally so testcontainers can pull `postgres:16`:
```bash
docker info >/dev/null 2>&1 && echo "DOCKER_UP" || echo "START_DOCKER_FIRST"
```
Expected: `DOCKER_UP`.

- [ ] **Step 4: Run the smoke test, watch it pass**

Run: `go test -race ./internal/store/... -run TestStoreNewAndPing -v`
Expected: PASS — `--- PASS: TestStoreNewAndPing` then `ok  github.com/marwanbukhori/go-brainstorming/internal/store` (first run pulls the `postgres:16` image; subsequent runs are fast).

- [ ] **Step 5: Commit**
```bash
git add internal/store/postgres_test.go
git commit -m "test: store smoke test (testcontainers postgres New + Pool.Ping)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Entrypoint — cmd/fuelpos/main.go

**Files:**
- Create: `cmd/fuelpos/main.go`

**Interfaces:**
- Consumes: `store.New(ctx, dsn) (*Store, error)`, `(*Store).Migrate() error`, `(*Store).Close()` (Task 5); env var `DATABASE_URL`; stdlib `log/slog`.
- Produces: the runnable binary `cmd/fuelpos` invoked by `make run` / `make migrate` — connects the pool, runs migrations, logs `ready`.

> This task is entrypoint wiring, not TDD. There is no unit test for `main`; Steps 1–2 are a build/vet gate proving the package does not yet build, and Step 4 confirms it compiles. `main` calls `Migrate()`, but at the end of Phase 0 the migrations FS is still empty, so a real `make run` against a live DB is only expected to succeed AFTER Phase 1 adds `0001`. The Phase-0 acceptance for this task is that the entrypoint COMPILES and `go vet` passes; do not gate Phase 0 on `make run` against Postgres (that becomes green in Phase 1). Do not expect a real unit-test failure here.

- [ ] **Step 1: Verification (infra gate, not a TDD red phase)**

The entrypoint is verified by build + vet (no unit test for `main`). This is a build/wiring gate, not a TDD red phase: prove it does not build yet.

Run: `go build ./cmd/fuelpos/... 2>&1 || echo "EXIT=$?"`
Expected: gate fails — `package github.com/marwanbukhori/go-brainstorming/cmd/fuelpos: no Go files`.

- [ ] **Step 2: Run the gate, confirm the package is absent**

Run: `go vet ./cmd/fuelpos/... 2>&1 || echo "EXIT=$?"`
Expected: gate fails — `no Go files in .../cmd/fuelpos`. Expected pre-wiring state, not a TDD red.

- [ ] **Step 3: Implement**

`cmd/fuelpos/main.go`:
```go
// Command fuelpos is the local entrypoint for the fuel-POS money core: it reads
// DATABASE_URL, opens the store, runs migrations to the latest version, and logs
// "ready". It grows in later plans; for now it proves the wiring boots.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/marwanbukhori/go-brainstorming/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is not set")
		os.Exit(1)
	}

	ctx := context.Background()

	s, err := store.New(ctx, dsn)
	if err != nil {
		logger.Error("connect store", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	if err := s.Migrate(); err != nil {
		logger.Error("run migrations", "err", err)
		os.Exit(1)
	}

	logger.Info("ready", "component", "fuelpos")
}
```

- [ ] **Step 4: Run the gate, confirm it passes**

Run: `go build ./cmd/fuelpos/... && go vet ./cmd/fuelpos/...`
Expected: PASS — both succeed with no output (binary compiles; `slog` "ready" path is wired). Whole-tree gate: `go build ./...` succeeds.

- [ ] **Step 5: Commit**
```bash
git add cmd/fuelpos/main.go
git commit -m "feat: cmd/fuelpos entrypoint (DATABASE_URL, New, Migrate, slog ready)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase 1 — Transaction State Machine & Optimistic Concurrency (version-CAS)

This phase builds the transaction aggregate, its legal-transition engine, and the persistence layer whose single most important line is `UPDATE transactions SET ... WHERE id=$id AND version=$expected`. That SQL — and the INSERT — each live in exactly ONE place: the tx-scoped cores `CreateTransactionTx` and `ApplyTransitionTx`. The ctx-only `CreateTransaction` / `ApplyTransition` are thin wrappers that BEGIN/COMMIT their own pgx transaction around those cores, so later phases (idempotency in Phase 2, capture in Phase 3) can call the `*Tx` cores inside a shared transaction without ever copy-pasting the safety-critical SQL. We prove — against a real Postgres via testcontainers, run with `-race` — that two concurrent conflicting transitions on one row produce exactly one winner and one `ErrVersionConflict`, and that the `one_active_txn_per_pump` partial unique index (ADR-004) lets only one live transaction exist per pump. No idempotency, no ledger, no acquirer yet: just whether two concurrent events can corrupt one transaction's state (the answer must be: no). All tasks build on Phase 0 (which already created `go.mod` with module `github.com/marwanbukhori/go-brainstorming`, `internal/store/postgres.go` with `Store`/`New`/`Close`/`Migrate`, the embedded migrations FS, and the `internal/money` package).

---

### Task 1: Status enum + IsTerminal()

**Files:**
- Create: `internal/transaction/status.go`
- Test: `internal/transaction/status_test.go`

**Interfaces:**
- Consumes: nothing (leaf package).
- Produces:
  - `type Status string`
  - Constants: `StatusAuthorizing`, `StatusAuthorized`, `StatusDispensing`, `StatusCompleted`, `StatusCapturing`, `StatusCaptured`, `StatusSettled`, `StatusDeclined`, `StatusVoided`, `StatusExpired`, `StatusReversed`, `StatusFailed` (values are the upper-case strings in the contracts, e.g. `StatusAuthorizing Status = "AUTHORIZING"`).
  - `func (s Status) IsTerminal() bool` — true for the terminal set `{SETTLED, DECLINED, VOIDED, EXPIRED, REVERSED, FAILED}`, used by the `one_active_txn_per_pump` partial index predicate (ADR-004).

- [ ] **Step 1: Write the failing test**

```go
package transaction

import "testing"

func TestStatusIsTerminal(t *testing.T) {
	terminal := []Status{
		StatusSettled, StatusDeclined, StatusVoided,
		StatusExpired, StatusReversed, StatusFailed,
	}
	nonTerminal := []Status{
		StatusAuthorizing, StatusAuthorized, StatusDispensing,
		StatusCompleted, StatusCapturing, StatusCaptured,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("Status(%q).IsTerminal() = false, want true", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("Status(%q).IsTerminal() = true, want false", s)
		}
	}
}

func TestStatusValues(t *testing.T) {
	cases := map[Status]string{
		StatusAuthorizing: "AUTHORIZING",
		StatusAuthorized:  "AUTHORIZED",
		StatusDispensing:  "DISPENSING",
		StatusCompleted:   "COMPLETED",
		StatusCapturing:   "CAPTURING",
		StatusCaptured:    "CAPTURED",
		StatusSettled:     "SETTLED",
		StatusDeclined:    "DECLINED",
		StatusVoided:      "VOIDED",
		StatusExpired:     "EXPIRED",
		StatusReversed:    "REVERSED",
		StatusFailed:      "FAILED",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("status const = %q, want %q", string(got), want)
		}
	}
}
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/transaction/... -run 'TestStatusIsTerminal|TestStatusValues' -v`
Expected: FAIL — build error `undefined: StatusSettled` (and the other constants / `IsTerminal`), because `status.go` does not exist yet.

- [ ] **Step 3: Implement**

```go
package transaction

// Status is the lifecycle state of a fuel transaction aggregate.
type Status string

const (
	StatusAuthorizing Status = "AUTHORIZING"
	StatusAuthorized  Status = "AUTHORIZED"
	StatusDispensing  Status = "DISPENSING"
	StatusCompleted   Status = "COMPLETED"
	StatusCapturing   Status = "CAPTURING"
	StatusCaptured    Status = "CAPTURED"
	StatusSettled     Status = "SETTLED"
	StatusDeclined    Status = "DECLINED"
	StatusVoided      Status = "VOIDED"
	StatusExpired     Status = "EXPIRED"
	StatusReversed    Status = "REVERSED"
	StatusFailed      Status = "FAILED"
)

// IsTerminal reports whether the status is one of the terminal set
// (SETTLED, DECLINED, VOIDED, EXPIRED, REVERSED, FAILED). This is the exact
// predicate negated by the one_active_txn_per_pump partial unique index
// (ADR-004): a row counts as "live" for the per-pump uniqueness constraint
// iff its status is NOT terminal.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSettled, StatusDeclined, StatusVoided,
		StatusExpired, StatusReversed, StatusFailed:
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/transaction/... -run 'TestStatusIsTerminal|TestStatusValues' -v`
Expected: PASS (`--- PASS: TestStatusIsTerminal`, `--- PASS: TestStatusValues`, `ok`).

- [ ] **Step 5: Commit**

```bash
git add internal/transaction/status.go internal/transaction/status_test.go
git commit -m "feat: add transaction Status enum with IsTerminal()

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Event enum + Apply() transition table

**Files:**
- Create: `internal/transaction/event.go`
- Create: `internal/transaction/statemachine.go`
- Test: `internal/transaction/statemachine_test.go`

**Interfaces:**
- Consumes: `Status` and its constants from Task 1.
- Produces:
  - `type Event string`
  - Constants: `EventAuthorize`, `EventAcquirerApproved`, `EventAcquirerDeclined`, `EventStartDispense`, `EventPumpStopped`, `EventCapture`, `EventAcquirerCaptured`, `EventHoldTimeout`, `EventCancel`, `EventReverse`, `EventIncludeInBatch` (values are the Go-style strings in the contracts, e.g. `EventAuthorize Event = "Authorize"`).
  - `var ErrIllegalTransition = errors.New("illegal transition")`
  - `func Apply(from Status, e Event) (Status, error)` — pure function returning the destination `Status` for a legal `(from,event)` pair, or `ErrIllegalTransition`. The bootstrap transition uses the zero `Status("")`: `Apply("", EventAuthorize) == (StatusAuthorizing, nil)`. Amount guards are NOT enforced here.

- [ ] **Step 1: Write the failing test**

```go
package transaction

import (
	"errors"
	"testing"
)

func TestEventValues(t *testing.T) {
	cases := map[Event]string{
		EventAuthorize:        "Authorize",
		EventAcquirerApproved: "AcquirerApproved",
		EventAcquirerDeclined: "AcquirerDeclined",
		EventStartDispense:    "StartDispense",
		EventPumpStopped:      "PumpStopped",
		EventCapture:          "Capture",
		EventAcquirerCaptured: "AcquirerCaptured",
		EventHoldTimeout:      "HoldTimeout",
		EventCancel:           "Cancel",
		EventReverse:          "Reverse",
		EventIncludeInBatch:   "IncludeInBatch",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("event const = %q, want %q", string(got), want)
		}
	}
}

func TestApplyLegalTransitions(t *testing.T) {
	legal := []struct {
		from Status
		ev   Event
		to   Status
	}{
		{Status(""), EventAuthorize, StatusAuthorizing},
		{StatusAuthorizing, EventAcquirerApproved, StatusAuthorized},
		{StatusAuthorizing, EventAcquirerDeclined, StatusDeclined},
		{StatusAuthorized, EventStartDispense, StatusDispensing},
		{StatusAuthorized, EventHoldTimeout, StatusExpired},
		{StatusAuthorized, EventCancel, StatusVoided},
		{StatusDispensing, EventPumpStopped, StatusCompleted},
		{StatusCompleted, EventCapture, StatusCapturing},
		{StatusCapturing, EventAcquirerCaptured, StatusCaptured},
		{StatusCaptured, EventIncludeInBatch, StatusSettled},
		{StatusCaptured, EventReverse, StatusReversed},
	}
	for _, c := range legal {
		got, err := Apply(c.from, c.ev)
		if err != nil {
			t.Errorf("Apply(%q,%q) returned err %v, want nil", c.from, c.ev, err)
			continue
		}
		if got != c.to {
			t.Errorf("Apply(%q,%q) = %q, want %q", c.from, c.ev, got, c.to)
		}
	}
}

func TestApplyIllegalTransitions(t *testing.T) {
	illegal := []struct {
		from Status
		ev   Event
	}{
		{Status(""), EventCapture},               // bootstrap only accepts Authorize
		{StatusAuthorizing, EventStartDispense},  // must be approved first
		{StatusAuthorized, EventCapture},         // must dispense+complete first
		{StatusDispensing, EventCapture},         // must stop the pump first
		{StatusCompleted, EventAcquirerCaptured}, // must request Capture first
		{StatusCaptured, EventAuthorize},         // already past authorize
		{StatusSettled, EventReverse},            // terminal, no transitions out
		{StatusDeclined, EventAuthorize},         // terminal
		{StatusVoided, EventCapture},             // terminal
		{StatusExpired, EventStartDispense},      // terminal
		{StatusReversed, EventCapture},           // terminal
		{StatusFailed, EventAuthorize},           // terminal
	}
	for _, c := range illegal {
		got, err := Apply(c.from, c.ev)
		if !errors.Is(err, ErrIllegalTransition) {
			t.Errorf("Apply(%q,%q) err = %v, want ErrIllegalTransition", c.from, c.ev, err)
		}
		if got != Status("") {
			t.Errorf("Apply(%q,%q) status = %q, want zero on error", c.from, c.ev, got)
		}
	}
}
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/transaction/... -run 'TestEventValues|TestApply' -v`
Expected: FAIL — build error `undefined: EventAuthorize` / `undefined: Apply` / `undefined: ErrIllegalTransition`, because `event.go` and `statemachine.go` do not exist yet.

- [ ] **Step 3: Implement**

Create `internal/transaction/event.go`:

```go
package transaction

// Event is an input that may drive the transaction state machine.
type Event string

const (
	EventAuthorize        Event = "Authorize"
	EventAcquirerApproved Event = "AcquirerApproved"
	EventAcquirerDeclined Event = "AcquirerDeclined"
	EventStartDispense    Event = "StartDispense"
	EventPumpStopped      Event = "PumpStopped"
	EventCapture          Event = "Capture"
	EventAcquirerCaptured Event = "AcquirerCaptured"
	EventHoldTimeout      Event = "HoldTimeout"
	EventCancel           Event = "Cancel"
	EventReverse          Event = "Reverse"
	EventIncludeInBatch   Event = "IncludeInBatch"
)
```

Create `internal/transaction/statemachine.go`:

```go
package transaction

import "errors"

// ErrIllegalTransition is returned by Apply for any (from,event) pair that is
// not in the legal transition table.
var ErrIllegalTransition = errors.New("illegal transition")

// transitionKey is the lookup key for the legal-transition table.
type transitionKey struct {
	from Status
	ev   Event
}

// transitions encodes the full legal-transition table (v1 doc §4). The zero
// Status ("") is the bootstrap state from which only Authorize is legal.
var transitions = map[transitionKey]Status{
	{Status(""), EventAuthorize}:               StatusAuthorizing,
	{StatusAuthorizing, EventAcquirerApproved}: StatusAuthorized,
	{StatusAuthorizing, EventAcquirerDeclined}: StatusDeclined,
	{StatusAuthorized, EventStartDispense}:     StatusDispensing,
	{StatusAuthorized, EventHoldTimeout}:       StatusExpired,
	{StatusAuthorized, EventCancel}:            StatusVoided,
	{StatusDispensing, EventPumpStopped}:       StatusCompleted,
	{StatusCompleted, EventCapture}:            StatusCapturing,
	{StatusCapturing, EventAcquirerCaptured}:   StatusCaptured,
	{StatusCaptured, EventIncludeInBatch}:      StatusSettled,
	{StatusCaptured, EventReverse}:             StatusReversed,
}

// Apply returns the destination status for a legal (from,event) pair, or
// ErrIllegalTransition. It is a pure function over the transition table;
// amount guards (e.g. captured <= auth) are enforced at the store/service
// layer, not here. On error it returns the zero Status.
func Apply(from Status, e Event) (Status, error) {
	to, ok := transitions[transitionKey{from: from, ev: e}]
	if !ok {
		return Status(""), ErrIllegalTransition
	}
	return to, nil
}
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/transaction/... -run 'TestEventValues|TestApply' -v`
Expected: PASS (`--- PASS: TestEventValues`, `--- PASS: TestApplyLegalTransitions`, `--- PASS: TestApplyIllegalTransitions`, `ok`).

- [ ] **Step 5: Commit**

```bash
git add internal/transaction/event.go internal/transaction/statemachine.go internal/transaction/statemachine_test.go
git commit -m "feat: add transaction Event enum and Apply transition table

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Transaction aggregate struct

**Files:**
- Create: `internal/transaction/transaction.go`
- Test: `internal/transaction/transaction_test.go`

**Interfaces:**
- Consumes: `Status` (Task 1); `money.Amount` from `github.com/marwanbukhori/go-brainstorming/internal/money` (Phase 0).
- Produces:
  - `type Transaction struct{ ID uuid.UUID; PumpID string; Status Status; FuelGrade string; AuthAmount money.Amount; CapturedAmount money.Amount; VolumeML int64; AcquirerRef string; IdempotencyKey string; Version int64; CreatedAt time.Time; UpdatedAt time.Time }` — the aggregate persisted and loaded by the store layer.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/transaction/... -run TestTransactionFields -v`
Expected: FAIL — build error `undefined: Transaction`, because `transaction.go` does not exist yet.

- [ ] **Step 3: Implement**

```go
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
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/transaction/... -run TestTransactionFields -v`
Expected: PASS (`--- PASS: TestTransactionFields`, `ok`). Also run the whole package — `go test -race ./internal/transaction/...` — and expect `ok` (all of Tasks 1–3 green).

- [ ] **Step 5: Commit**

```bash
git add internal/transaction/transaction.go internal/transaction/transaction_test.go
git commit -m "feat: add Transaction aggregate struct

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: 0001_transactions migration + CreateTransactionTx / CreateTransaction / GetTransaction

**Files:**
- Create: `internal/store/migrations/0001_transactions.up.sql`
- Create: `internal/store/migrations/0001_transactions.down.sql`
- Create: `internal/store/transactions.go`
- Test: `internal/store/transactions_test.go`

**Interfaces:**
- Consumes: `Store` with field `Pool *pgxpool.Pool` and methods `New(ctx, dsn) (*Store, error)`, `Close()`, `Migrate() error` (Phase 0); `transaction.Transaction`, `transaction.Status`, status constants (Tasks 1–3); `money.Amount`; `uuid.UUID`; `pgx.Tx` from `github.com/jackc/pgx/v5`.
- Produces:
  - `var ErrNotFound = errors.New("transaction not found")`
  - `func (s *Store) CreateTransactionTx(ctx context.Context, tx pgx.Tx, t *transaction.Transaction) error` — the SINGLE INSERT implementation. Binds ALL columns (incl. `t.IdempotencyKey -> idempotency_key`) and writes `version = 1`, using the caller's `tx`. On success, back-fills `t.Version`, `t.CreatedAt`, `t.UpdatedAt` from the DB. Phase 2's idempotency service calls this directly to insert the transaction in the SAME tx as the key claim.
  - `func (s *Store) CreateTransaction(ctx context.Context, t *transaction.Transaction) error` — the ctx-only wrapper: BEGIN, call `CreateTransactionTx`, COMMIT (rollback on error). The only insert SQL lives in `CreateTransactionTx`; this method has none.
  - `func (s *Store) GetTransaction(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)` — returns the row (ALL columns incl. `created_at`/`updated_at`), or `ErrNotFound` when the id does not exist. Not-found is detected via `errors.Is(err, pgx.ErrNoRows)`.
- Note for later tasks: the test helper `newTestStore(ctx, t) *Store` defined here (a testcontainers `postgres:16` + `Migrate()`) is reused by Tasks 5 and 6 — they each define their own copy in their own `_test.go` file because Go test files are not shared across separate edits; the helper body is identical and shown in full each time.

- [ ] **Step 1: Write the failing test**

Create `internal/store/transactions_test.go`:

```go
package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newTestStore spins a throwaway postgres:16 container, opens a Store against
// it, runs all migrations, and registers cleanup. It skips the test if Docker
// is unavailable.
func newTestStore(ctx context.Context, t *testing.T) *Store {
	t.Helper()
	ctr, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("fuelpos"),
		tcpostgres.WithUsername("fuelpos"),
		tcpostgres.WithPassword("fuelpos"),
		tcpostgres.BasicWaitStrategies(),
		wait.ForListeningPort("5432/tcp"),
	)
	if err != nil {
		t.Skipf("could not start postgres container (is Docker running?): %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

func newTxn(pumpID string) *transaction.Transaction {
	return &transaction.Transaction{
		ID:             uuid.New(),
		PumpID:         pumpID,
		Status:         transaction.StatusAuthorizing,
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(15000),
		CapturedAmount: money.Amount(0),
		VolumeML:       0,
		AcquirerRef:    "",
		IdempotencyKey: uuid.NewString(),
	}
}

// tableExists reports whether the named table is present in the public schema.
func tableExists(ctx context.Context, t *testing.T, s *Store, name string) bool {
	t.Helper()
	var ok bool
	err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=$1)`,
		name,
	).Scan(&ok)
	if err != nil {
		t.Fatalf("query table existence for %q: %v", name, err)
	}
	return ok
}

func TestMigrateRunsClean(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	// Migrate() already ran inside newTestStore; calling it again must be a no-op.
	if err := s.Migrate(); err != nil {
		t.Fatalf("second Migrate() should be a clean no-op, got: %v", err)
	}
	if !tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table was not created by Migrate()")
	}
}

// TestMigrateRedo exercises the .down.sql files: migrate fully Down (dropping
// every table) then Up again, asserting the schema is restored. This proves the
// 0001 down migration actually drops the transactions table and that a fresh Up
// rebuilds it (a one-way migration that silently no-ops on Down would pass the
// happy path but fail here).
func TestMigrateRedo(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	// Sanity: schema is up after newTestStore.
	if !tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table missing before redo")
	}

	dsn := s.Pool.Config().ConnString()
	m, err := migrate.New("file://migrations", dsn)
	if err != nil {
		t.Fatalf("migrate.New: %v", err)
	}
	defer m.Close()

	// Down to a clean slate: the transactions table must be gone.
	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate Down: %v", err)
	}
	if tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table still present after migrate Down; 0001 down.sql did not drop it")
	}

	// Up again: the schema must be fully restored.
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate Up after Down: %v", err)
	}
	if !tableExists(ctx, t, s, "transactions") {
		t.Fatal("transactions table not restored after Down then Up")
	}

	// And it must be usable again (a re-created, empty table).
	in := newTxn("pump-redo-1")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction after redo: %v", err)
	}
}

func TestCreateAndGetTransaction(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-1")
	before := time.Now().Add(-time.Minute)
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	if in.Version != 1 {
		t.Errorf("after create, Version = %d, want 1", in.Version)
	}
	if in.CreatedAt.Before(before) || in.CreatedAt.IsZero() {
		t.Errorf("CreatedAt not back-filled: %v", in.CreatedAt)
	}

	got, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if got.ID != in.ID {
		t.Errorf("ID = %v, want %v", got.ID, in.ID)
	}
	if got.PumpID != "pump-1" {
		t.Errorf("PumpID = %q, want pump-1", got.PumpID)
	}
	if got.Status != transaction.StatusAuthorizing {
		t.Errorf("Status = %q, want AUTHORIZING", got.Status)
	}
	if got.AuthAmount != money.Amount(15000) {
		t.Errorf("AuthAmount = %d, want 15000", int64(got.AuthAmount))
	}
	if got.IdempotencyKey != in.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, in.IdempotencyKey)
	}
	if got.Version != 1 {
		t.Errorf("Version = %d, want 1", got.Version)
	}
}

func TestGetTransactionNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	_, err := s.GetTransaction(ctx, uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetTransaction(missing) err = %v, want ErrNotFound", err)
	}
}
```

Create `internal/store/migrations/0001_transactions.up.sql` (verbatim from the contracts):

```sql
CREATE TABLE transactions (
    id                    uuid        PRIMARY KEY,
    pump_id               text        NOT NULL,
    status                text        NOT NULL,
    fuel_grade            text        NOT NULL DEFAULT '',
    auth_amount_minor     bigint      NOT NULL DEFAULT 0,
    captured_amount_minor bigint      NOT NULL DEFAULT 0,
    volume_ml             bigint      NOT NULL DEFAULT 0,
    acquirer_ref          text        NOT NULL DEFAULT '',
    idempotency_key       text        NOT NULL,
    version               bigint      NOT NULL DEFAULT 1,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);
-- ADR-004: one live transaction per pump, enforced by a partial unique index (not a lock service)
CREATE UNIQUE INDEX one_active_txn_per_pump ON transactions (pump_id)
    WHERE status NOT IN ('SETTLED','DECLINED','VOIDED','EXPIRED','REVERSED','FAILED');
```

Create `internal/store/migrations/0001_transactions.down.sql`:

```sql
DROP TABLE transactions;
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/store/... -run 'TestMigrateRunsClean|TestMigrateRedo|TestCreateAndGetTransaction|TestGetTransactionNotFound' -v`
Expected: FAIL — build error `undefined: (*store.Store).CreateTransaction` / `undefined: (*store.Store).CreateTransactionTx` / `undefined: store.ErrNotFound` (the `0001` migration exists but `transactions.go` does not).

- [ ] **Step 3: Implement**

Create `internal/store/transactions.go`:

```go
package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// ErrNotFound is returned when a transaction row does not exist.
var ErrNotFound = errors.New("transaction not found")

// CreateTransactionTx is the SINGLE insert implementation. It binds ALL columns
// (including t.IdempotencyKey -> idempotency_key) and writes version = 1, using
// the caller's tx. On success the DB-assigned version/timestamps are written
// back into t. Callers that must insert the transaction in the same tx as
// another effect (Phase 2 idempotency: claim key + insert txn atomically) call
// this directly; the ctx-only CreateTransaction wraps it with BEGIN/COMMIT.
func (s *Store) CreateTransactionTx(ctx context.Context, tx pgx.Tx, t *transaction.Transaction) error {
	const q = `
INSERT INTO transactions (
    id, pump_id, status, fuel_grade,
    auth_amount_minor, captured_amount_minor, volume_ml,
    acquirer_ref, idempotency_key, version
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1)
RETURNING version, created_at, updated_at`
	return tx.QueryRow(ctx, q,
		t.ID, t.PumpID, string(t.Status), t.FuelGrade,
		int64(t.AuthAmount), int64(t.CapturedAmount), t.VolumeML,
		t.AcquirerRef, t.IdempotencyKey,
	).Scan(&t.Version, &t.CreatedAt, &t.UpdatedAt)
}

// CreateTransaction is the ctx-only wrapper around CreateTransactionTx: it
// BEGINs a transaction, inserts via the single core, and COMMITs (rolling back
// on error). The insert SQL exists only in CreateTransactionTx.
func (s *Store) CreateTransaction(ctx context.Context, t *transaction.Transaction) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := s.CreateTransactionTx(ctx, tx, t); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// GetTransaction loads a single transaction by id (all columns), or ErrNotFound.
// Not-found is detected via errors.Is(err, pgx.ErrNoRows).
func (s *Store) GetTransaction(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	const q = `
SELECT id, pump_id, status, fuel_grade,
       auth_amount_minor, captured_amount_minor, volume_ml,
       acquirer_ref, idempotency_key, version, created_at, updated_at
FROM transactions
WHERE id = $1`

	var (
		t       transaction.Transaction
		status  string
		authMin int64
		capMin  int64
	)
	err := s.Pool.QueryRow(ctx, q, id).Scan(
		&t.ID, &t.PumpID, &status, &t.FuelGrade,
		&authMin, &capMin, &t.VolumeML,
		&t.AcquirerRef, &t.IdempotencyKey, &t.Version, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.Status = transaction.Status(status)
	t.AuthAmount = money.Amount(authMin)
	t.CapturedAmount = money.Amount(capMin)
	return &t, nil
}
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/store/... -run 'TestMigrateRunsClean|TestMigrateRedo|TestCreateAndGetTransaction|TestGetTransactionNotFound' -v`
Expected: PASS (`--- PASS: TestMigrateRunsClean`, `--- PASS: TestMigrateRedo`, `--- PASS: TestCreateAndGetTransaction`, `--- PASS: TestGetTransactionNotFound`, `ok`). Requires Docker running; if absent the tests SKIP rather than fail.

- [ ] **Step 5: Commit**

```bash
git add internal/store/migrations/0001_transactions.up.sql internal/store/migrations/0001_transactions.down.sql internal/store/transactions.go internal/store/transactions_test.go
git commit -m "feat: add transactions migration with CreateTransactionTx wrapper and GetTransaction

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: ApplyTransitionTx (version-CAS core) + ApplyTransition wrapper

**Files:**
- Modify: `internal/store/transactions.go` (append `ApplyTransitionTx`, `ApplyTransition`, and `ErrVersionConflict`)
- Test: `internal/store/apply_transition_test.go`

**Interfaces:**
- Consumes: `CreateTransaction`, `GetTransaction`, `ErrNotFound` (Task 4); `transaction.Apply` and `transaction.ErrIllegalTransition` (Task 2); `transaction.Transaction` (Task 3); `pgx.Tx` from `github.com/jackc/pgx/v5`.
- Produces:
  - `var ErrVersionConflict = errors.New("optimistic version conflict")`
  - `func (s *Store) ApplyTransitionTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, expectedVersion int64, e transaction.Event, mutate func(*transaction.Transaction)) (*transaction.Transaction, error)` — the SINGLE version-CAS implementation. It loads the row in the caller's `tx` (ALL columns incl. `created_at`/`updated_at`; not-found via `errors.Is(err, pgx.ErrNoRows)` returning `ErrNotFound`), computes the next status via `transaction.Apply` (returns `ErrIllegalTransition` if illegal), lets `mutate` adjust amount/acquirer fields, then runs the ONLY copy of `UPDATE transactions SET status=$next, version=version+1, ... WHERE id=$id AND version=$expectedVersion`, returning `ErrVersionConflict` if 0 rows were affected. On success returns the updated transaction with `Version == expectedVersion+1`. Phase 3's capture path calls this directly to fold the state change into the same tx as the ledger posting.
  - `func (s *Store) ApplyTransition(ctx context.Context, id uuid.UUID, expectedVersion int64, e transaction.Event, mutate func(*transaction.Transaction)) (*transaction.Transaction, error)` — the ctx-only wrapper: BEGIN, call `ApplyTransitionTx`, COMMIT (rollback on error). The `WHERE id=$id AND version=$expectedVersion` SQL exists ONLY in `ApplyTransitionTx`; this method has none.

- [ ] **Step 1: Write the failing test**

Create `internal/store/apply_transition_test.go`:

```go
package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

func TestApplyTransitionHappyPath(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-apply-1")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}

	// AUTHORIZING --AcquirerApproved--> AUTHORIZED, recording the acquirer ref.
	out, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventAcquirerApproved,
		func(tx *transaction.Transaction) { tx.AcquirerRef = "auth-ref-9" })
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	if out.Status != transaction.StatusAuthorized {
		t.Errorf("Status = %q, want AUTHORIZED", out.Status)
	}
	if out.Version != 2 {
		t.Errorf("Version = %d, want 2", out.Version)
	}
	if out.AcquirerRef != "auth-ref-9" {
		t.Errorf("AcquirerRef = %q, want auth-ref-9", out.AcquirerRef)
	}

	reloaded, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if reloaded.Status != transaction.StatusAuthorized || reloaded.Version != 2 {
		t.Errorf("persisted = (%q,v%d), want (AUTHORIZED,v2)", reloaded.Status, reloaded.Version)
	}
}

func TestApplyTransitionStaleVersionConflict(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-apply-2")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	// First transition wins, moving version 1 -> 2.
	if _, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventAcquirerApproved, func(*transaction.Transaction) {}); err != nil {
		t.Fatalf("first ApplyTransition: %v", err)
	}
	// Second call still claims expectedVersion 1 -> must conflict.
	_, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventStartDispense, func(*transaction.Transaction) {})
	if !errors.Is(err, ErrVersionConflict) {
		t.Errorf("stale ApplyTransition err = %v, want ErrVersionConflict", err)
	}
}

func TestApplyTransitionIllegalEvent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-apply-3")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}
	// AUTHORIZING --Capture--> illegal.
	_, err := s.ApplyTransition(ctx, in.ID, 1, transaction.EventCapture, func(*transaction.Transaction) {})
	if !errors.Is(err, transaction.ErrIllegalTransition) {
		t.Errorf("illegal ApplyTransition err = %v, want ErrIllegalTransition", err)
	}
	// State must be untouched.
	reloaded, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if reloaded.Status != transaction.StatusAuthorizing || reloaded.Version != 1 {
		t.Errorf("after illegal event = (%q,v%d), want (AUTHORIZING,v1)", reloaded.Status, reloaded.Version)
	}
}

func TestApplyTransitionNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	_, err := s.ApplyTransition(ctx, uuid.New(), 1, transaction.EventAcquirerApproved, func(*transaction.Transaction) {})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("ApplyTransition(missing) err = %v, want ErrNotFound", err)
	}
	_ = money.Amount(0) // money import kept consistent across store tests
}
```

- [ ] **Step 2: Run the test, watch it fail**

Run: `go test -race ./internal/store/... -run 'TestApplyTransition' -v`
Expected: FAIL — build error `undefined: (*store.Store).ApplyTransition` / `undefined: (*store.Store).ApplyTransitionTx` / `undefined: store.ErrVersionConflict`.

- [ ] **Step 3: Implement**

Append to `internal/store/transactions.go` (add `ErrVersionConflict` near `ErrNotFound`, and both methods at the end of the file):

```go
// ErrVersionConflict is returned by ApplyTransitionTx when the optimistic CAS
// matched 0 rows because a concurrent writer advanced the version first.
var ErrVersionConflict = errors.New("optimistic version conflict")

// ApplyTransitionTx is the SINGLE version-CAS implementation. Using the caller's
// tx it loads the row (ALL columns, via GetTransactionTx — see below), computes
// the next status via transaction.Apply, lets mutate() adjust amount/acquirer
// fields, then performs the ONLY copy of the optimistic CAS:
//
//	UPDATE transactions SET status=$next, version=version+1, ... WHERE id=$id AND version=$expectedVersion
//
// It returns ErrVersionConflict if 0 rows were affected (a concurrent writer
// won), transaction.ErrIllegalTransition if the event is not legal from the
// current state, or ErrNotFound if the row does not exist. Callers that must
// fold the state change into another effect (Phase 3 capture: state change +
// ledger posting atomically) call this directly; the ctx-only ApplyTransition
// wraps it with BEGIN/COMMIT.
func (s *Store) ApplyTransitionTx(
	ctx context.Context,
	tx pgx.Tx,
	id uuid.UUID,
	expectedVersion int64,
	e transaction.Event,
	mutate func(*transaction.Transaction),
) (*transaction.Transaction, error) {
	const loadQ = `
SELECT id, pump_id, status, fuel_grade,
       auth_amount_minor, captured_amount_minor, volume_ml,
       acquirer_ref, idempotency_key, version, created_at, updated_at
FROM transactions
WHERE id = $1`

	var (
		current transaction.Transaction
		status  string
		authMin int64
		capMin  int64
	)
	err := tx.QueryRow(ctx, loadQ, id).Scan(
		&current.ID, &current.PumpID, &status, &current.FuelGrade,
		&authMin, &capMin, &current.VolumeML,
		&current.AcquirerRef, &current.IdempotencyKey, &current.Version,
		&current.CreatedAt, &current.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	current.Status = transaction.Status(status)
	current.AuthAmount = money.Amount(authMin)
	current.CapturedAmount = money.Amount(capMin)

	next, err := transaction.Apply(current.Status, e)
	if err != nil {
		return nil, err
	}

	current.Status = next
	if mutate != nil {
		mutate(&current)
	}

	const updateQ = `
UPDATE transactions
SET status = $1,
    fuel_grade = $2,
    auth_amount_minor = $3,
    captured_amount_minor = $4,
    volume_ml = $5,
    acquirer_ref = $6,
    version = version + 1,
    updated_at = now()
WHERE id = $7 AND version = $8
RETURNING version, updated_at`

	err = tx.QueryRow(ctx, updateQ,
		string(current.Status), current.FuelGrade,
		int64(current.AuthAmount), int64(current.CapturedAmount), current.VolumeML,
		current.AcquirerRef,
		id, expectedVersion,
	).Scan(&current.Version, &current.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrVersionConflict
	}
	if err != nil {
		return nil, err
	}
	return &current, nil
}

// ApplyTransition is the ctx-only wrapper around ApplyTransitionTx: it BEGINs a
// transaction, runs the single CAS core, and COMMITs (rolling back on error).
// The WHERE id=$id AND version=$expectedVersion SQL exists only in
// ApplyTransitionTx.
func (s *Store) ApplyTransition(
	ctx context.Context,
	id uuid.UUID,
	expectedVersion int64,
	e transaction.Event,
	mutate func(*transaction.Transaction),
) (*transaction.Transaction, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	out, err := s.ApplyTransitionTx(ctx, tx, id, expectedVersion, e, mutate)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/store/... -run 'TestApplyTransition' -v`
Expected: PASS (`--- PASS: TestApplyTransitionHappyPath`, `--- PASS: TestApplyTransitionStaleVersionConflict`, `--- PASS: TestApplyTransitionIllegalEvent`, `--- PASS: TestApplyTransitionNotFound`, `ok`).

- [ ] **Step 5: Commit**

```bash
git add internal/store/transactions.go internal/store/apply_transition_test.go
git commit -m "feat: add ApplyTransitionTx version-CAS core and ApplyTransition wrapper

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Concurrency regression / safety-net — racing CAS + one_active_txn_per_pump

**Files:**
- Test: `internal/store/concurrency_test.go`

**Interfaces:**
- Consumes: `CreateTransaction`, `GetTransaction` (Task 4); `ApplyTransition`, `ErrVersionConflict` (Task 5); `transaction.Event`/`transaction.Status` constants; the `one_active_txn_per_pump` partial unique index from the `0001` migration (Task 4). This task adds NO production code — the version-CAS protection was already implemented in Task 5 (`ApplyTransitionTx`'s `WHERE id=$id AND version=$expectedVersion`) and the per-pump uniqueness in Task 4's DDL. This is a REGRESSION / SAFETY-NET test that locks in those guarantees under real concurrency; it is NOT a fresh TDD red phase (there is no missing production code for it to drive out).
- Produces: nothing consumed by later phases; it is the gate that closes Phase 1.

- [ ] **Step 1: Write the regression test**

Create `internal/store/concurrency_test.go`:

```go
package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// TestConcurrentApplyTransitionExactlyOneWinner fires two conflicting
// transitions at the same row, both claiming expectedVersion 1. Exactly one
// must succeed (version -> 2); the other must get ErrVersionConflict. This is a
// regression test that guards Task 5's version-CAS under real concurrency.
func TestConcurrentApplyTransitionExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	in := newTxn("pump-race-1")
	if err := s.CreateTransaction(ctx, in); err != nil {
		t.Fatalf("CreateTransaction: %v", err)
	}

	// Two legal-from-AUTHORIZING events racing on version 1.
	events := []transaction.Event{
		transaction.EventAcquirerApproved, // -> AUTHORIZED
		transaction.EventAcquirerDeclined, // -> DECLINED
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		conflicts int
	)
	wg.Add(len(events))
	for _, e := range events {
		e := e
		go func() {
			defer wg.Done()
			_, err := s.ApplyTransition(ctx, in.ID, 1, e, func(*transaction.Transaction) {})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrVersionConflict):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Errorf("successes = %d, want exactly 1", successes)
	}
	if conflicts != 1 {
		t.Errorf("conflicts = %d, want exactly 1", conflicts)
	}

	final, err := s.GetTransaction(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if final.Version != 2 {
		t.Errorf("final Version = %d, want 2 (exactly one writer advanced it)", final.Version)
	}
}

// TestSecondActiveTxnPerPumpViolatesIndex guards ADR-004: a second live
// (non-terminal) transaction for a pump that already has a live transaction is
// rejected by the one_active_txn_per_pump partial unique index.
func TestSecondActiveTxnPerPumpViolatesIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)

	first := newTxn("pump-serial-1") // AUTHORIZING (non-terminal => live)
	if err := s.CreateTransaction(ctx, first); err != nil {
		t.Fatalf("first CreateTransaction: %v", err)
	}

	second := newTxn("pump-serial-1") // same pump, also live
	err := s.CreateTransaction(ctx, second)
	if err == nil {
		t.Fatal("second live transaction for the same pump was accepted; want unique violation")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("want unique_violation (SQLSTATE 23505) on one_active_txn_per_pump, got: %v", err)
	}
	if !strings.Contains(pgErr.ConstraintName, "one_active_txn_per_pump") {
		t.Errorf("violated constraint = %q, want one_active_txn_per_pump", pgErr.ConstraintName)
	}

	// Drive the first transaction to a terminal state, then a new live txn for
	// the same pump must be allowed (the partial index only covers live rows).
	if _, err := s.ApplyTransition(ctx, first.ID, 1, transaction.EventAcquirerDeclined, func(*transaction.Transaction) {}); err != nil {
		t.Fatalf("decline first txn: %v", err)
	}
	third := newTxn("pump-serial-1")
	if err := s.CreateTransaction(ctx, third); err != nil {
		t.Errorf("after first txn terminal, a new live txn for the pump should be allowed, got: %v", err)
	}
}
```

- [ ] **Step 2: Run the test, watch it pass (regression — it is green by construction)**

Run: `go test -race ./internal/store/... -run 'TestConcurrentApplyTransitionExactlyOneWinner|TestSecondActiveTxnPerPumpViolatesIndex' -v`
Expected: PASS. This is a regression / safety-net test: the protections it asserts (Task 5's version-CAS predicate and Task 4's `one_active_txn_per_pump` index) already exist, so it is green by construction — there is no genuine TDD red phase here. To observe it genuinely fail (optional, for understanding only), temporarily revert the version predicate by deleting `AND version = $8` from the `updateQ` `WHERE` clause in `ApplyTransitionTx` (or drop the `one_active_txn_per_pump` index in `0001_transactions.up.sql`) and re-run: expect `successes = 2, want exactly 1` for the CAS test and `second live transaction for the same pump was accepted` for the index test. Restore the predicate/index before continuing.

- [ ] **Step 3: Implement**

No production code is required: the safety properties already live in Task 4's DDL (`one_active_txn_per_pump`) and Task 5's CAS (`WHERE id=$id AND version=$expectedVersion` in `ApplyTransitionTx`). If the optional Step 2 breakage was applied to observe a failure, restore `internal/store/transactions.go` and `internal/store/migrations/0001_transactions.up.sql` to their committed state:

```bash
git checkout -- internal/store/transactions.go internal/store/migrations/0001_transactions.up.sql
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -race ./internal/store/... -run 'TestConcurrentApplyTransitionExactlyOneWinner|TestSecondActiveTxnPerPumpViolatesIndex' -v`
Expected: PASS (`--- PASS: TestConcurrentApplyTransitionExactlyOneWinner`, `--- PASS: TestSecondActiveTxnPerPumpViolatesIndex`, `ok`). Then run the full unit + integration sweep to close the phase: `go test -race ./internal/money/... ./internal/transaction/... ./internal/store/...` — expect `ok` for every package.

- [ ] **Step 5: Commit**

```bash
git add internal/store/concurrency_test.go
git commit -m "test: regression-guard version-CAS and one_active_txn_per_pump under concurrency

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase 2 — Atomic Idempotency (ADR-001)

This phase makes retries safe: a pump that re-sends the same `Authorize` with the same idempotency key gets back the same transaction and exactly one effect. The non-negotiable rule of ADR-001 is that **claiming the key and committing the effect are the same Postgres transaction** — never a dual-write across stores, never a second effect on replay. We build the `0002_idempotency_keys` migration (the `key` PRIMARY KEY *is* the dedupe mechanism), a tx-scoped `ClaimIdempotencyKey` that does `INSERT ... ON CONFLICT (key) DO NOTHING` and reports whether *this* caller won the claim, and a small `internal/service` authorize function that wires `BEGIN → claim → (if claimed) CreateTransactionTx → COMMIT` so a crash can never persist the key without the effect. The effect is persisted via the canonical `(*Store).CreateTransactionTx(ctx, tx, t)` inside the claim's tx — the same insert core Phase 1 uses — with `t.IdempotencyKey = req.IdempotencyKey` so the stored `idempotency_key` is the business key, not the row id. We prove it four ways against real Postgres (testcontainers, `-race`): claim-twice returns the stored snapshot the second time; a forced ROLLBACK after a claim leaves *neither* the key row *nor* the transaction row (crash-atomicity); the same key twice yields exactly one transaction row with the same id; and 16 concurrent identical requests resolve to exactly one effect.

### Task 1: Migration `0002_idempotency_keys`

**Files:**
- Create: `internal/store/migrations/0002_idempotency_keys.up.sql`
- Create: `internal/store/migrations/0002_idempotency_keys.down.sql`
- Test: `internal/store/idempotency_migration_test.go`

**Interfaces:**
- Consumes: `store.New(ctx, dsn) (*Store, error)`, `(*Store).Migrate() error`, `(*Store).Close()`, `(*Store).Pool *pgxpool.Pool` (Phase 0/1 contracts). The embedded migration file source under `internal/store/migrations/` (Phase 0) automatically picks up `0002_*` files.
- Produces: the `idempotency_keys` table (columns `key`, `request_hash`, `transaction_id`, `response_snapshot`, `created_at`) reachable after `Migrate()`. Later tasks insert into it via `ClaimIdempotencyKey`.

- [ ] **Step 1: Write the failing test**
```go
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres spins a throwaway postgres:16 and returns a migrated Store.
// Shared helper for all Phase 2 integration tests in package store_test.
func startPostgres(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()

	pg, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("fuelpos"),
		postgres.WithUsername("fuelpos"),
		postgres.WithPassword("fuelpos"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	s, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)

	if err := s.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func TestIdempotencyKeysTableExists(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	var regclass *string
	err := s.Pool.QueryRow(ctx, `SELECT to_regclass('idempotency_keys')::text`).Scan(&regclass)
	if err != nil {
		t.Fatalf("query to_regclass: %v", err)
	}
	if regclass == nil || *regclass != "idempotency_keys" {
		t.Fatalf("idempotency_keys table missing after Migrate(); to_regclass=%v", regclass)
	}

	// The PRIMARY KEY on key is the dedupe mechanism (ADR-001): assert it exists.
	var n int
	err = s.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.table_constraints
		WHERE table_name = 'idempotency_keys' AND constraint_type = 'PRIMARY KEY'`).Scan(&n)
	if err != nil {
		t.Fatalf("query pk: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 PRIMARY KEY on idempotency_keys, got %d", n)
	}
}
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/store/... -run TestIdempotencyKeysTableExists -v`
Expected: FAIL — the test compiles and the container starts, but `Migrate()` has no `0002_*` files so `to_regclass('idempotency_keys')` returns NULL: `idempotency_keys table missing after Migrate(); to_regclass=<nil>`.
- [ ] **Step 3: Implement**

`internal/store/migrations/0002_idempotency_keys.up.sql`:
```sql
CREATE TABLE idempotency_keys (
    key               text        PRIMARY KEY,           -- the UNIQUE constraint IS the dedupe mechanism (ADR-001)
    request_hash      text        NOT NULL,
    transaction_id    uuid        NOT NULL,
    response_snapshot jsonb,
    created_at        timestamptz NOT NULL DEFAULT now()
);
```

`internal/store/migrations/0002_idempotency_keys.down.sql`:
```sql
DROP TABLE idempotency_keys;
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/store/... -run TestIdempotencyKeysTableExists -v`
Expected: PASS
- [ ] **Step 5: Commit**
```bash
git add internal/store/migrations/0002_idempotency_keys.up.sql \
        internal/store/migrations/0002_idempotency_keys.down.sql \
        internal/store/idempotency_migration_test.go
git commit -m "feat: add 0002_idempotency_keys migration (ADR-001)

The key PRIMARY KEY is the dedupe mechanism for atomic idempotency.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `ClaimIdempotencyKey` (same-txn claim)

**Files:**
- Create: `internal/store/idempotency.go`
- Test: `internal/store/idempotency_test.go`

**Interfaces:**
- Consumes: `(*Store).Pool *pgxpool.Pool`; `pgx.Tx` from `Pool.Begin(ctx)`; the `idempotency_keys` table from Task 1; `uuid.UUID` (`github.com/google/uuid`); `pgx.ErrNoRows` / `errors.Is`.
- Produces:
  - `type IdempotencySnapshot struct { TransactionID uuid.UUID; ResponseJSON []byte }`
  - `func (s *Store) ClaimIdempotencyKey(ctx context.Context, tx pgx.Tx, key, requestHash string, txnID uuid.UUID) (existing *IdempotencySnapshot, claimed bool, err error)` — `INSERT ... ON CONFLICT (key) DO NOTHING` inside the passed `tx`. If the row was inserted: returns `(nil, true, nil)`. If the key already existed: returns `(existing, false, nil)` where `existing.TransactionID` / `existing.ResponseJSON` come from the stored row. Tasks 3+ (the authorize service and the crash-atomicity test) call this.

- [ ] **Step 1: Write the failing test**
```go
package store_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestClaimIdempotencyKey_FirstClaimsSecondReturnsSnapshot(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	key := "idem-key-abc"
	reqHash := "hash-v1"
	txnID := uuid.New()

	// First claim: inside its own tx, inserts the row and returns claimed=true.
	tx1, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	existing, claimed, err := s.ClaimIdempotencyKey(ctx, tx1, key, reqHash, txnID)
	if err != nil {
		t.Fatalf("first ClaimIdempotencyKey: %v", err)
	}
	if !claimed {
		t.Fatalf("first claim: want claimed=true, got false")
	}
	if existing != nil {
		t.Fatalf("first claim: want existing=nil, got %+v", existing)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	// Second claim, same key, different txnID: row already exists, returns the STORED snapshot.
	tx2, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	existing, claimed, err = s.ClaimIdempotencyKey(ctx, tx2, key, reqHash, uuid.New())
	if err != nil {
		t.Fatalf("second ClaimIdempotencyKey: %v", err)
	}
	if claimed {
		t.Fatalf("second claim: want claimed=false, got true")
	}
	if existing == nil {
		t.Fatalf("second claim: want non-nil snapshot, got nil")
	}
	if existing.TransactionID != txnID {
		t.Fatalf("second claim: snapshot TransactionID = %s, want the first-claim id %s", existing.TransactionID, txnID)
	}
}
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/store/... -run TestClaimIdempotencyKey_FirstClaimsSecondReturnsSnapshot -v`
Expected: FAIL to compile — `s.ClaimIdempotencyKey undefined (type *store.Store has no field or method ClaimIdempotencyKey)` and `undefined: store.IdempotencySnapshot` (the symbols don't exist yet).
- [ ] **Step 3: Implement**

`internal/store/idempotency.go`:
```go
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// IdempotencySnapshot is the stored result of a previously-claimed key.
// On a duplicate claim the caller returns this instead of re-executing the effect.
type IdempotencySnapshot struct {
	TransactionID uuid.UUID
	ResponseJSON  []byte
}

// ClaimIdempotencyKey attempts INSERT ... ON CONFLICT (key) DO NOTHING within tx
// (ADR-001: the claim must live in the SAME transaction as the effect it guards).
//
//   - If the row was inserted, this caller won the claim: returns (nil, true, nil)
//     and the caller proceeds to perform the effect inside the same tx.
//   - If the key already existed, the effect already happened (or is in flight under
//     another committed tx): returns (existing snapshot, false, nil) and the caller
//     returns the stored result WITHOUT re-executing.
func (s *Store) ClaimIdempotencyKey(ctx context.Context, tx pgx.Tx, key, requestHash string, txnID uuid.UUID) (existing *IdempotencySnapshot, claimed bool, err error) {
	// RETURNING only yields a row when the INSERT actually inserted (no conflict).
	var insertedKey string
	err = tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (key, request_hash, transaction_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO NOTHING
		RETURNING key`,
		key, requestHash, txnID,
	).Scan(&insertedKey)
	if err == nil {
		// We inserted the row: this caller owns the claim.
		return nil, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("claim idempotency key: %w", err)
	}

	// Conflict: the key already exists. Load the stored snapshot.
	snap := &IdempotencySnapshot{}
	err = tx.QueryRow(ctx, `
		SELECT transaction_id, response_snapshot
		FROM idempotency_keys
		WHERE key = $1`,
		key,
	).Scan(&snap.TransactionID, &snap.ResponseJSON)
	if err != nil {
		return nil, false, fmt.Errorf("load existing idempotency snapshot: %w", err)
	}
	return snap, false, nil
}
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/store/... -run TestClaimIdempotencyKey_FirstClaimsSecondReturnsSnapshot -v`
Expected: PASS
- [ ] **Step 5: Commit**
```bash
git add internal/store/idempotency.go internal/store/idempotency_test.go
git commit -m "feat: add ClaimIdempotencyKey same-txn claim (ADR-001)

INSERT ... ON CONFLICT (key) DO NOTHING inside the passed pgx.Tx;
returns claimed=true on insert, or the stored snapshot on conflict.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Crash-atomicity proof — rollback after claim leaves NOTHING (ADR-001)

**Files:**
- Test: `internal/store/idempotency_atomicity_test.go`

**Interfaces:**
- Consumes: `startPostgres(t)` helper (Task 1, same `store_test` package); `(*Store).ClaimIdempotencyKey` (Task 2); `(*Store).CreateTransactionTx(ctx, tx, t)` and `(*Store).GetTransaction(ctx, id)` (Phase 1 contracts); `transaction.Transaction`, `transaction.StatusAuthorizing`; `money.Amount`; `uuid`; `pgx.Tx` from `Pool.Begin`; `store.ErrNotFound` / `errors.Is`.
- Produces: no new production symbols — this is the ADR-001 crash-atomicity proof. It claims the key AND writes the effect inside one tx, then forces a ROLLBACK (simulating a crash before COMMIT) and asserts that *both* the `idempotency_keys` row is ABSENT *and* the `transactions` row does not exist. This proves the key can never persist without its effect: they share one atomic boundary.

- [ ] **Step 1: Write the failing test**
```go
package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// errInjectedCrash simulates a process crash (or any error) after the claim and
// the effect were written, but BEFORE COMMIT. Under ADR-001 the rollback that
// follows must discard BOTH writes atomically.
var errInjectedCrash = errors.New("injected crash before commit")

func TestClaimThenRollback_LeavesNeitherKeyNorTransaction(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	key := "pump-9-authorize-crash"
	txnID := uuid.New()

	// One tx: claim the key, write the effect, then DO NOT commit — roll back.
	err := func() error {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		// Guarantees a rollback on every return path of this closure.
		defer func() { _ = tx.Rollback(ctx) }()

		_, claimed, err := s.ClaimIdempotencyKey(ctx, tx, key, "hash-v1", txnID)
		if err != nil {
			return err
		}
		if !claimed {
			t.Fatalf("setup: expected to win the claim on a fresh db")
		}

		// The effect: persist the transaction via the canonical tx-aware insert,
		// inside the SAME tx as the claim (ADR-001).
		if err := s.CreateTransactionTx(ctx, tx, &transaction.Transaction{
			ID:             txnID,
			PumpID:         "pump-9",
			Status:         transaction.StatusAuthorizing,
			FuelGrade:      "RON95",
			AuthAmount:     money.Amount(15000),
			IdempotencyKey: key,
		}); err != nil {
			return err
		}

		// Crash before COMMIT: return an error so the deferred Rollback fires.
		return errInjectedCrash
	}()
	if !errors.Is(err, errInjectedCrash) {
		t.Fatalf("want injected crash error to propagate, got %v", err)
	}

	// PROOF 1: the idempotency_keys row is ABSENT — the claim did not persist.
	var keys int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&keys); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if keys != 0 {
		t.Fatalf("want 0 idempotency_keys rows after rollback, got %d", keys)
	}

	// PROOF 2: no transactions row exists — the effect did not persist either.
	if _, err := s.GetTransaction(ctx, txnID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound for rolled-back transaction, got %v", err)
	}
}
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/store/... -run TestClaimThenRollback_LeavesNeitherKeyNorTransaction -v`
Expected: FAIL — initially RED only if `CreateTransactionTx` is missing or `ErrNotFound` is unwired; with the Phase 1 contracts in place this test compiles and passes immediately, so the honest red→green here is the **regression guard**: it locks in that the claim and the effect share one tx. If it ever goes green only because the writes leak across separate transactions, the cardinality assertions (`keys==0`, `ErrNotFound`) catch it. Run it once to confirm GREEN against the real atomic path before committing.
- [ ] **Step 3: Implement** — no production change required.
This task adds the crash-atomicity safety net on top of the Phase 1 `CreateTransactionTx` core and the Task 2 claim. If the test does not pass, the bug is that the claim or the effect is escaping the shared tx — fix that in `internal/store` (it must NOT be re-introduced as a private helper); do not weaken the test.
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/store/... -run TestClaimThenRollback_LeavesNeitherKeyNorTransaction -v`
Expected: PASS — `keys == 0` and `GetTransaction` returns `store.ErrNotFound`. The rollback discarded both writes atomically.
- [ ] **Step 5: Commit**
```bash
git add internal/store/idempotency_atomicity_test.go
git commit -m "test: prove rollback after claim+effect leaves neither row (ADR-001)

One tx: ClaimIdempotencyKey then CreateTransactionTx, then forced rollback.
Asserts the key row is absent AND the transaction is ErrNotFound — the key
can never persist without its effect because they share one atomic boundary.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Atomic `Authorize` service (claim + effect in one tx)

**Files:**
- Create: `internal/service/authorize.go`
- Test: `internal/service/authorize_test.go`

**Interfaces:**
- Consumes:
  - `store.Store` with `Pool *pgxpool.Pool`, `(*Store).ClaimIdempotencyKey(ctx, tx, key, requestHash, txnID) (*IdempotencySnapshot, bool, error)` (Task 2), and `(*Store).CreateTransactionTx(ctx, tx, t) error` (Phase 1 contracts — the ONE canonical tx-aware insert core; no private re-implementation).
  - `transaction.Transaction` aggregate, `transaction.StatusAuthorizing` (Phase 1 contracts).
  - `money.Amount` (Phase 1), `uuid` (`github.com/google/uuid`), `pgx`/`pgxpool`, `pgconn` (`github.com/jackc/pgx/v5/pgconn`) for the unique-violation classification.
- Produces:
  - `type AuthorizeRequest struct { IdempotencyKey string; RequestHash string; PumpID string; FuelGrade string; AuthAmount money.Amount }`
  - `type Authorizer struct { Store *store.Store }`
  - `func NewAuthorizer(s *store.Store) *Authorizer`
  - `func (a *Authorizer) Authorize(ctx context.Context, req AuthorizeRequest) (uuid.UUID, error)` — runs `BEGIN → ClaimIdempotencyKey → if claimed CreateTransactionTx (the effect) in the SAME tx → COMMIT`; if not claimed, returns the stored `TransactionID` from the snapshot WITHOUT a second effect. The transaction is built with `IdempotencyKey = req.IdempotencyKey` (the business key) so the persisted `idempotency_key` column binds to the dedupe key, not the row id. A goroutine that loses the unique-insert race on COMMIT resolves the stored id instead of erroring. Task 5 (the concurrency safety net) calls this.

- [ ] **Step 1: Write the failing test**
```go
package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/service"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func newAuthorizer(t *testing.T) (*service.Authorizer, *store.Store) {
	t.Helper()
	ctx := context.Background()

	pg, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("fuelpos"),
		postgres.WithUsername("fuelpos"),
		postgres.WithPassword("fuelpos"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	s, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return service.NewAuthorizer(s), s
}

func TestAuthorize_SameKeyTwice_OneRowSameID(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)

	req := service.AuthorizeRequest{
		IdempotencyKey: "pump-3-authorize-001",
		RequestHash:    "hash-v1",
		PumpID:         "pump-3",
		FuelGrade:      "RON95",
		AuthAmount:     money.Amount(15000), // RM150.00
	}

	id1, err := a.Authorize(ctx, req)
	if err != nil {
		t.Fatalf("first authorize: %v", err)
	}

	// Replay the identical request (same idempotency key): must dedupe to the same id.
	id2, err := a.Authorize(ctx, req)
	if err != nil {
		t.Fatalf("second authorize (replay): %v", err)
	}
	if id1 != id2 {
		t.Fatalf("replay returned different id: first=%s second=%s", id1, id2)
	}

	// Exactly ONE transaction row exists for this pump: the effect ran once.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM transactions WHERE pump_id = $1`, req.PumpID).Scan(&n); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if n != 1 {
		t.Fatalf("want exactly 1 transaction row, got %d", n)
	}

	// Exactly ONE idempotency_keys row was claimed, bound to the BUSINESS key.
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, req.IdempotencyKey).Scan(&n); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if n != 1 {
		t.Fatalf("want exactly 1 idempotency_keys row, got %d", n)
	}

	// The persisted transactions.idempotency_key MUST be the business key
	// (req.IdempotencyKey), proving the effect bound to the dedupe key, not the row id.
	var storedKey string
	if err := s.Pool.QueryRow(ctx,
		`SELECT idempotency_key FROM transactions WHERE id = $1`, id1).Scan(&storedKey); err != nil {
		t.Fatalf("read stored idempotency_key: %v", err)
	}
	if storedKey != req.IdempotencyKey {
		t.Fatalf("transactions.idempotency_key = %q, want the business key %q", storedKey, req.IdempotencyKey)
	}
}
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/service/... -run TestAuthorize_SameKeyTwice_OneRowSameID -v`
Expected: FAIL to compile — `undefined: service.NewAuthorizer`, `undefined: service.Authorizer`, `undefined: service.AuthorizeRequest` (the `internal/service` package does not exist yet).
- [ ] **Step 3: Implement**

`internal/service/authorize.go`:
```go
package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// AuthorizeRequest is one business intent to authorize a fuel sale.
// IdempotencyKey is one key per intent; replays carry the same key.
type AuthorizeRequest struct {
	IdempotencyKey string
	RequestHash    string
	PumpID         string
	FuelGrade      string
	AuthAmount     money.Amount
}

// Authorizer wires the ADR-001 atomic-idempotency pattern over the Store.
type Authorizer struct {
	Store *store.Store
}

// NewAuthorizer constructs an Authorizer.
func NewAuthorizer(s *store.Store) *Authorizer {
	return &Authorizer{Store: s}
}

// Authorize realises ADR-001: claim-the-key and commit-the-effect in ONE
// Postgres transaction so a crash can never persist the key without the effect.
//
//	BEGIN
//	  ClaimIdempotencyKey(key)
//	  if claimed:   CreateTransactionTx(...)   -- the effect, SAME tx
//	  else:         return the stored transaction id          -- NO second effect
//	COMMIT
//
// On replay with the same key, the claim conflicts and the stored transaction id
// is returned without re-running the effect. A goroutine that wins its in-tx claim
// but loses the COMMIT race to a concurrent winner resolves the stored id instead
// of erroring.
func (a *Authorizer) Authorize(ctx context.Context, req AuthorizeRequest) (uuid.UUID, error) {
	tx, err := a.Store.Pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin tx: %w", err)
	}
	// Rollback is a no-op after a successful Commit.
	defer func() { _ = tx.Rollback(ctx) }()

	txnID := uuid.New()

	existing, claimed, err := a.Store.ClaimIdempotencyKey(ctx, tx, req.IdempotencyKey, req.RequestHash, txnID)
	if err != nil {
		return uuid.Nil, err
	}
	if !claimed {
		// Duplicate intent: the effect already committed under another tx.
		// Return the stored id; do NOT perform a second effect.
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, fmt.Errorf("commit (dedup): %w", err)
		}
		return existing.TransactionID, nil
	}

	// We own the claim: persist the effect via the canonical tx-aware insert,
	// in the SAME tx as the claim (ADR-001). IdempotencyKey is the BUSINESS key
	// (req.IdempotencyKey) so the stored idempotency_key binds to the dedupe key.
	if err := a.Store.CreateTransactionTx(ctx, tx, &transaction.Transaction{
		ID:             txnID,
		PumpID:         req.PumpID,
		Status:         transaction.StatusAuthorizing,
		FuelGrade:      req.FuelGrade,
		AuthAmount:     req.AuthAmount,
		IdempotencyKey: req.IdempotencyKey,
	}); err != nil {
		return uuid.Nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		// A concurrent winner committed the same key first; our claim was
		// optimistic. Rollback is already deferred; re-resolve the stored id.
		if isUniqueViolation(err) {
			return a.resolveExisting(ctx, req.IdempotencyKey)
		}
		return uuid.Nil, fmt.Errorf("commit (effect): %w", err)
	}
	return txnID, nil
}

// isUniqueViolation reports whether err is a Postgres unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// resolveExisting reads back the committed transaction id for a key whose
// claim we lost to a concurrent winner.
func (a *Authorizer) resolveExisting(ctx context.Context, key string) (uuid.UUID, error) {
	var id uuid.UUID
	err := a.Store.Pool.QueryRow(ctx,
		`SELECT transaction_id FROM idempotency_keys WHERE key = $1`, key).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("resolve existing id: %w", err)
	}
	return id, nil
}
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/service/... -run TestAuthorize_SameKeyTwice_OneRowSameID -v`
Expected: PASS — one transaction row, one idempotency_keys row, replay returns the same id, and `transactions.idempotency_key` equals the business key.
- [ ] **Step 5: Commit**
```bash
git add internal/service/authorize.go internal/service/authorize_test.go
git commit -m "feat: atomic Authorize service (claim + effect one tx, ADR-001)

BEGIN -> ClaimIdempotencyKey -> if claimed CreateTransactionTx -> COMMIT.
The effect is persisted via the canonical tx-aware insert with
IdempotencyKey=req.IdempotencyKey, so the stored idempotency_key binds to
the business dedupe key. Same key twice yields one transaction row, same id.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Concurrency safety net — 16 identical requests, exactly one effect (ADR-001)

**Files:**
- Test: `internal/service/authorize_concurrency_test.go`

**Interfaces:**
- Consumes: `service.NewAuthorizer`, `service.AuthorizeRequest`, `(*Authorizer).Authorize(ctx, req) (uuid.UUID, error)` (Task 4); the `newAuthorizer(t)` helper from `authorize_test.go` (same `service_test` package); `money.Amount`; `uuid`.
- Produces: no new production symbols — this is the ADR-001 concurrency regression/safety-net test. Task 4 already makes the loser path deterministic (it resolves the stored id), so this test has a CRISP deterministic expectation: 16 goroutines firing the identical request (same idempotency key) at once produce **exactly one** `transactions` row, **exactly one** `idempotency_keys` row, **every** call returns nil error, and **all 16** returned ids are identical.

- [ ] **Step 1: Write the failing test**
```go
package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/service"
)

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

	var start sync.WaitGroup // released together to maximise the race
	var done sync.WaitGroup
	start.Add(1)
	done.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer done.Done()
			start.Wait()
			ids[i], errs[i] = a.Authorize(ctx, req)
		}(i)
	}

	start.Done() // fire all goroutines at once
	done.Wait()

	// DETERMINISTIC EXPECTATION 1: every caller returns nil error — the loser of
	// the unique-insert race resolves the stored id, it never surfaces as an error.
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
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/service/... -run TestAuthorize_ConcurrentSameKey_EffectRunsOnce -v`
Expected: FAIL to compile first if `authorize_concurrency_test.go` is added before the file is saved alongside Task 4's package; once it compiles against the Task 4 `Authorize`, it is GREEN. This is a regression/safety-net test: Task 4 already made the loser path deterministic, so there is no separate red production change here. The honest RED is only observed if Task 4's `isUniqueViolation`/`resolveExisting` loser-handling is removed or broken — then losing goroutines surface a `duplicate key value violates unique constraint "idempotency_keys_pkey"` error and Expectation 1 fails. Run it once to confirm it guards that behaviour.
- [ ] **Step 3: Implement** — no production change required.
Task 4 already encodes the deterministic loser path (`isUniqueViolation` → `resolveExisting`). This task is the standing safety net. If it fails, fix `Authorize` in `internal/service/authorize.go` (never weaken the test or re-introduce a private insert helper — the effect must go through `CreateTransactionTx`).
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/service/... -run TestAuthorize_ConcurrentSameKey_EffectRunsOnce -v`
Expected: PASS — `txns == 1`, `keys == 1`, all 16 goroutines return nil error and the same id. Then run the full phase suite to confirm no regression:
Run: `go test -race ./internal/store/... ./internal/service/... -v`
Expected: PASS (all Phase 2 tests).
- [ ] **Step 5: Commit**
```bash
git add internal/service/authorize_concurrency_test.go
git commit -m "test: 16 concurrent identical requests run the effect once (ADR-001)

Safety net: 16 goroutines, same idempotency key -> exactly 1 transaction row,
1 key row, every caller returns nil error and the same id. Guards the
deterministic loser-resolution path in Authorize.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase 3 — Append-Only Double-Entry Ledger & Invariant Fold (ADR-002/003/010)

This phase adds the money-defining half of the spine: an **append-only double-entry ledger** whose per-transaction invariant (`Σ debits = Σ credits`) is enforced in pure Go before any write, and whose global invariant is proven by a **fold/SUM over the ledger** that survives the future shard-by-pump migration (ADR-010 framing). First we build the pure `internal/ledger` types and `Balanced(entries)` under full unit TDD (balanced passes; debit-only, credit-only, empty, and non-positive amounts are rejected). Then we add the `0003_ledger_entries` migration (append-only, a `CHECK (amount_minor > 0)`, no balance column per ADR-003), a tx-scoped `PostEntries(ctx, tx, entries)` that runs `ledger.Balanced` first (`ErrUnbalanced`) then bulk-inserts so an unbalanced batch writes **nothing**, and `FoldInvariant(ctx)` returning `(debits, credits)`. Finally we deliver the headline demonstration of ADR-002: extend the capture service so a single `pgx.Tx` performs **both** the `COMPLETED → CAPTURING → CAPTURED` state transition (via the store's `ApplyTransitionTx`) **and** the balanced ledger posting (DEBIT `cash-clearing`, CREDIT `fuel-revenue` for the captured amount) — so after a capture the transaction is `CAPTURED` and the fold shows `debits == credits` with no captured-but-unbooked window. The capture service also enforces the `captured <= auth_amount` HARD guard (`ErrCaptureExceedsAuth`) at the service layer before stamping, posting nothing on a violation. There is ONE version-CAS implementation — the store's `ApplyTransitionTx` — reused inside the shared capture tx; no duplicated transition helper, no exported test shim, no compile anchor. Accounts are simple string constants; no sharding, no clearing-sweep, no `transaction_events` table (those are later plans — do NOT introduce them here).

### Task 1: Ledger `Direction` + `Entry` types

**Files:**
- Create: `internal/ledger/entry.go`
- Test: `internal/ledger/entry_test.go`

**Interfaces:**
- Consumes: `money.Amount` (`github.com/marwanbukhori/go-brainstorming/internal/money`, Phase 0); `uuid.UUID` (`github.com/google/uuid`).
- Produces:
  - `type Direction string` with `const ( Debit Direction = "DEBIT"; Credit Direction = "CREDIT" )`
  - `type Entry struct { TransactionID uuid.UUID; Account string; Direction Direction; Amount money.Amount }`
  Task 2 (`Balanced`), Task 4 (`PostEntries`), and Task 6 (capture service) all construct `ledger.Entry` values and read these fields.

- [ ] **Step 1: Write the failing test**
```go
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
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/ledger/... -run TestEntry_FieldsAndDirectionConstants -v`
Expected: FAIL to compile — `package github.com/marwanbukhori/go-brainstorming/internal/ledger is not in std` / `undefined: ledger.Debit`, `undefined: ledger.Credit`, `undefined: ledger.Entry` (the `internal/ledger` package does not exist yet).
- [ ] **Step 3: Implement**

`internal/ledger/entry.go`:
```go
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
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/ledger/... -run TestEntry_FieldsAndDirectionConstants -v`
Expected: PASS
- [ ] **Step 5: Commit**
```bash
git add internal/ledger/entry.go internal/ledger/entry_test.go
git commit -m "feat: add ledger Direction and Entry types (ADR-002/003)

Append-only double-entry primitives; Amount always > 0, Direction carries sign,
no balance column (balances are derived by folding).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `Balanced(entries)` — the per-transaction double-entry invariant

**Files:**
- Create: `internal/ledger/posting.go`
- Test: `internal/ledger/posting_test.go`

**Interfaces:**
- Consumes: `ledger.Entry`, `ledger.Direction`, `ledger.Debit`, `ledger.Credit` (Task 1); `money.Amount` (Phase 0) and its `Add` method.
- Produces:
  - `var ErrUnbalanced = errors.New("ledger entries not balanced: sum(debits) != sum(credits)")`
  - `func Balanced(entries []Entry) error` — returns `nil` iff the slice is non-empty, every `Amount > 0`, and `Σ DEBIT amounts == Σ CREDIT amounts`; otherwise `ErrUnbalanced`. Task 4 (`PostEntries`) calls `Balanced` before inserting.

- [ ] **Step 1: Write the failing test**
```go
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
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/ledger/... -run TestBalanced -v`
Expected: FAIL to compile — `undefined: ledger.Balanced` and `undefined: ledger.ErrUnbalanced`.
- [ ] **Step 3: Implement**

`internal/ledger/posting.go`:
```go
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
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/ledger/... -run TestBalanced -v`
Expected: PASS (all subtests).
- [ ] **Step 5: Commit**
```bash
git add internal/ledger/posting.go internal/ledger/posting_test.go
git commit -m "feat: add ledger.Balanced double-entry invariant (ADR-002)

nil iff non-empty, all amounts > 0, and sum(debits) == sum(credits);
otherwise ErrUnbalanced. Pure; rejects debit-only/credit-only/empty/non-positive.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Migration `0003_ledger_entries` (append-only)

**Files:**
- Create: `internal/store/migrations/0003_ledger_entries.up.sql`
- Create: `internal/store/migrations/0003_ledger_entries.down.sql`
- Test: `internal/store/ledger_migration_test.go`

**Interfaces:**
- Consumes: `store.New(ctx, dsn) (*Store, error)`, `(*Store).Migrate() error`, `(*Store).Close()`, `(*Store).Pool *pgxpool.Pool` (Phase 0 contracts); the `startPostgres(t)` helper already defined in `internal/store/idempotency_migration_test.go` (Phase 2 Task 1, same `store_test` package). The embedded migration file source under `internal/store/migrations/` automatically picks up the `0003_*` files.
- Produces: the `ledger_entries` table (columns `id`, `transaction_id`, `account`, `direction`, `amount_minor`, `created_at`) plus index `ledger_entries_txn_idx`, reachable after `Migrate()`. Task 4 (`PostEntries`) and Task 5 (`FoldInvariant`) read/write it.

- [ ] **Step 1: Write the failing test**
```go
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
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/store/... -run TestLedgerEntriesTableExists -v`
Expected: FAIL — the test compiles and the container starts, but `Migrate()` has no `0003_*` files so `to_regclass('ledger_entries')` returns NULL: `ledger_entries table missing after Migrate(); to_regclass=<nil>`.
- [ ] **Step 3: Implement**

`internal/store/migrations/0003_ledger_entries.up.sql` (verbatim from the contracts DDL):
```sql
CREATE TABLE ledger_entries (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id uuid        NOT NULL,
    account        text        NOT NULL,
    direction      text        NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),  -- ADR-003: no balance column; balances derived
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_txn_idx ON ledger_entries (transaction_id);
-- Append-only by convention (no UPDATE/DELETE in code). A revoke + trigger guard arrives in a later plan.
```

`internal/store/migrations/0003_ledger_entries.down.sql`:
```sql
DROP TABLE ledger_entries;
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/store/... -run TestLedgerEntriesTableExists -v`
Expected: PASS — table and index exist, and the two illegal INSERTs are both refused by the CHECK constraints.
- [ ] **Step 5: Commit**
```bash
git add internal/store/migrations/0003_ledger_entries.up.sql \
        internal/store/migrations/0003_ledger_entries.down.sql \
        internal/store/ledger_migration_test.go
git commit -m "feat: add 0003_ledger_entries migration (append-only, ADR-002/003)

CHECK (amount_minor > 0) and CHECK (direction IN ('DEBIT','CREDIT'));
no balance column (balances derived); ledger_entries_txn_idx for per-txn reads.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `PostEntries` — Balanced-then-bulk-insert in the caller's tx

**Files:**
- Create: `internal/store/ledger.go`
- Test: `internal/store/ledger_test.go`

**Interfaces:**
- Consumes: `(*Store).Pool *pgxpool.Pool`; `pgx.Tx` from `Pool.Begin(ctx)`; the `ledger_entries` table (Task 3); `ledger.Entry`, `ledger.Balanced`, `ledger.ErrUnbalanced` (Tasks 1–2); the `startPostgres(t)` helper (Phase 2 Task 1, `store_test`).
- Produces:
  - `func (s *Store) PostEntries(ctx context.Context, tx pgx.Tx, entries []ledger.Entry) error` — calls `ledger.Balanced(entries)` first (returns `ledger.ErrUnbalanced` on failure, **before any write**), then bulk-INSERTs every entry inside the passed `tx`. Append-only: never updates or deletes. Task 6 (capture service) calls this inside its capture tx. If `Balanced` fails, nothing is inserted; if the surrounding `tx` is rolled back by the caller, nothing is inserted either.

This is the first file in `internal/store/ledger.go`. It compiles with only `context`, `fmt`, `pgx`, and `ledger` — it does NOT import `money` (PostEntries never references `money.Amount`; it casts `int64(e.Amount)` at the call site). `money` is introduced cleanly in Task 5, the first task whose code actually references `money.Amount`. There is no scaffolding here that a later task deletes — every line committed in this task is final code.

- [ ] **Step 1: Write the failing test**
```go
package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func TestPostEntries_BalancedInserts_UnbalancedRejectsNothingInserted(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	// --- Balanced batch inserts both legs in one tx ---
	txnID := uuid.New()
	balanced := []ledger.Entry{
		{TransactionID: txnID, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(15000)},
		{TransactionID: txnID, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(15000)},
	}

	tx1, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	if err := s.PostEntries(ctx, tx1, balanced); err != nil {
		t.Fatalf("PostEntries(balanced): %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, txnID).Scan(&n); err != nil {
		t.Fatalf("count balanced entries: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 ledger entries for the balanced batch, got %d", n)
	}

	// --- Unbalanced batch is rejected with ErrUnbalanced and inserts NOTHING ---
	badTxnID := uuid.New()
	unbalanced := []ledger.Entry{
		{TransactionID: badTxnID, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(15000)},
		{TransactionID: badTxnID, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(14999)},
	}

	tx2, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	err = s.PostEntries(ctx, tx2, unbalanced)
	if !errors.Is(err, ledger.ErrUnbalanced) {
		_ = tx2.Rollback(ctx)
		t.Fatalf("PostEntries(unbalanced): want ErrUnbalanced, got %v", err)
	}
	// Even if the caller went on to commit, the rejected batch wrote nothing.
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2 (after rejected post): %v", err)
	}
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, badTxnID).Scan(&n); err != nil {
		t.Fatalf("count unbalanced entries: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 ledger entries after an unbalanced (rejected) batch, got %d", n)
	}
}
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/store/... -run TestPostEntries_BalancedInserts_UnbalancedRejectsNothingInserted -v`
Expected: FAIL to compile — `s.PostEntries undefined (type *store.Store has no field or method PostEntries)`.
- [ ] **Step 3: Implement**

`internal/store/ledger.go`:
```go
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
```
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/store/... -run TestPostEntries_BalancedInserts_UnbalancedRejectsNothingInserted -v`
Expected: PASS — balanced batch yields 2 rows; unbalanced batch returns `ErrUnbalanced` and yields 0 rows.
- [ ] **Step 5: Commit**
```bash
git add internal/store/ledger.go internal/store/ledger_test.go
git commit -m "feat: add PostEntries balanced-then-bulk-insert (ADR-002/003)

Calls ledger.Balanced first (ErrUnbalanced, nothing written) then bulk-inserts
in the caller's tx. Append-only: no UPDATE/DELETE.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `FoldInvariant` — global Σ debits / Σ credits

**Files:**
- Modify: `internal/store/ledger.go` (add `FoldInvariant` and its `money` import)
- Test: `internal/store/ledger_test.go` (add `TestFoldInvariant_DebitsEqualCreditsAfterBatch` and `TestFoldInvariant_DetectsOutOfBandUnbalancedInsert`)

**Interfaces:**
- Consumes: `(*Store).Pool *pgxpool.Pool`; the `ledger_entries` table (Task 3); `(*Store).PostEntries` (Task 4); `ledger.Entry`, `ledger.Debit`, `ledger.Credit`; `money.Amount` (Phase 0); the `startPostgres(t)` helper.
- Produces:
  - `func (s *Store) FoldInvariant(ctx context.Context) (debits money.Amount, credits money.Amount, err error)` — returns the global sums folded over `ledger_entries`. The continuous invariant is `debits == credits` (ADR-002). Expressed as a SUM/fold so it survives the future shard-by-pump migration (ADR-010 framing). Task 6 (the capture demonstration) calls this to assert the invariant holds after a capture.

This task introduces `FoldInvariant` — the first and only consumer of `money.Amount` in `internal/store/ledger.go` — and adds the `money` import alongside the function that uses it. Task 4's file compiled cleanly without `money`; this task adds `money` only because the new code actually references it. There is no compile anchor (no `var _ = money.Amount(0)`) and no scaffolding committed-then-deleted: every commit in this phase compiles cleanly with only final code.

- [ ] **Step 1: Write the failing tests**

Both tests go in `internal/store/ledger_test.go`. The first proves the happy-path fold; the second is the NEGATIVE test that demonstrates the fold is *necessary but not sufficient* — an out-of-band, unbalanced direct INSERT (bypassing `PostEntries`/`Balanced`) makes `FoldInvariant` report `debits != credits`. The table-level `CHECK` only guards `amount_minor > 0` and the direction token; it does NOT enforce balance. Balance is guarded by `PostEntries`/`Balanced`, and `FoldInvariant` is the global detector that surfaces any breach.

```go
package store_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)

func TestFoldInvariant_DebitsEqualCreditsAfterBatch(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	// Empty ledger: both sums are zero (and trivially equal).
	d0, c0, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant(empty): %v", err)
	}
	if d0 != money.Amount(0) || c0 != money.Amount(0) {
		t.Fatalf("empty ledger: want (0,0), got (%d,%d)", int64(d0), int64(c0))
	}

	// Post two balanced batches (a split-debit one to exercise the fold over many rows).
	txnA := uuid.New()
	txnB := uuid.New()
	batchA := []ledger.Entry{
		{TransactionID: txnA, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(15000)},
		{TransactionID: txnA, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(15000)},
	}
	batchB := []ledger.Entry{
		{TransactionID: txnB, Account: "cash-clearing", Direction: ledger.Debit, Amount: money.Amount(500)},
		{TransactionID: txnB, Account: "card-clearing", Direction: ledger.Debit, Amount: money.Amount(2000)},
		{TransactionID: txnB, Account: "fuel-revenue", Direction: ledger.Credit, Amount: money.Amount(2500)},
	}

	for _, batch := range [][]ledger.Entry{batchA, batchB} {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := s.PostEntries(ctx, tx, batch); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("PostEntries: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	debits, credits, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant: %v", err)
	}
	// The continuous invariant (ADR-002): debits == credits.
	if debits != credits {
		t.Fatalf("invariant broken: debits=%d credits=%d", int64(debits), int64(credits))
	}
	// And concretely: 15000 + (500+2000) = 17500 on each side.
	if debits != money.Amount(17500) {
		t.Fatalf("debits = %d, want 17500", int64(debits))
	}
	if credits != money.Amount(17500) {
		t.Fatalf("credits = %d, want 17500", int64(credits))
	}
}

// TestFoldInvariant_DetectsOutOfBandUnbalancedInsert proves the fold is
// NECESSARY-BUT-NOT-SUFFICIENT: balance is guarded by PostEntries/Balanced, NOT
// by the ledger_entries table CHECK (which only enforces amount_minor > 0 and
// the direction token). A direct, out-of-band INSERT that bypasses PostEntries
// can leave the ledger unbalanced, and FoldInvariant is what surfaces it
// (debits != credits). This is exactly why every legitimate write goes through
// PostEntries — and why the fold exists as the global detector.
func TestFoldInvariant_DetectsOutOfBandUnbalancedInsert(t *testing.T) {
	ctx := context.Background()
	s := startPostgres(t)

	// A lone DEBIT inserted directly — this passes the table CHECKs (amount > 0,
	// direction is 'DEBIT') yet has no matching CREDIT. PostEntries/Balanced would
	// have rejected it; the raw INSERT does not.
	badTxn := uuid.New()
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account, direction, amount_minor)
		VALUES ($1, 'cash-clearing', 'DEBIT', 15000)`, badTxn); err != nil {
		t.Fatalf("out-of-band insert: %v", err)
	}

	debits, credits, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant: %v", err)
	}
	// The fold SURFACES the breach: the table CHECK did not (and cannot) prevent it.
	if debits == credits {
		t.Fatalf("expected fold to detect imbalance, got debits=%d credits=%d (equal)", int64(debits), int64(credits))
	}
	if debits != money.Amount(15000) || credits != money.Amount(0) {
		t.Fatalf("want (debits=15000, credits=0), got (%d,%d)", int64(debits), int64(credits))
	}
}
```
- [ ] **Step 2: Run the tests, watch them fail**
Run: `go test -race ./internal/store/... -run 'TestFoldInvariant_' -v`
Expected: FAIL to compile — `s.FoldInvariant undefined (type *store.Store has no field or method FoldInvariant)`.
- [ ] **Step 3: Implement**

Append `FoldInvariant` to `internal/store/ledger.go` and add the `money` import (it is now referenced for the first time):
```go
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
)
```
```go
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
```
- [ ] **Step 4: Run the tests, watch them pass**
Run: `go test -race ./internal/store/... -run 'TestFoldInvariant_' -v`
Expected: PASS — empty ledger folds to (0,0); after the two balanced batches both sides fold to 17500 and `debits == credits`; the out-of-band lone-DEBIT case makes the fold report `debits=15000, credits=0` (imbalance detected).
- [ ] **Step 5: Commit**
```bash
git add internal/store/ledger.go internal/store/ledger_test.go
git commit -m "feat: add FoldInvariant global debits/credits fold (ADR-002/010)

SUM(...) FILTER over ledger_entries returning (debits, credits); continuous
invariant is debits == credits. Fold framing survives future shard-by-pump.
Negative test proves the fold is necessary-but-not-sufficient: an out-of-band
unbalanced INSERT passes the table CHECK yet the fold detects debits != credits
(balance is guarded by PostEntries/Balanced, not the CHECK).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Capture posts state + ledger in ONE tx (the ADR-002 demonstration)

**Files:**
- Create: `internal/service/capture.go`
- Test: `internal/service/capture_test.go`

**Interfaces:**
- Consumes:
  - `(*store.Store).ApplyTransitionTx(ctx, tx, id, expectedVersion, e, mutate) (*transaction.Transaction, error)` — the ONE version-CAS core (Phase 1 contract). The capture path reuses it inside the shared capture tx; there is NO separate transition helper and NO exported test shim in `internal/service`.
  - `(*store.Store).ApplyTransition(ctx, id, expectedVersion, e, mutate) (*transaction.Transaction, error)` — the ctx-only wrapper (Phase 1 contract), used by the test helper to drive the row to COMPLETED through the real state machine.
  - `(*store.Store).PostEntries(ctx, tx, entries) error` (Task 4); `(*store.Store).FoldInvariant(ctx) (money.Amount, money.Amount, error)` (Task 5).
  - `ledger.Entry`, `ledger.Debit`, `ledger.Credit` (Task 1); `money.Amount` (Phase 0).
  - `store.ErrCaptureExceedsAuth` (Phase 3 capture-guard contract).
  - `transaction.EventAuthorize`, `transaction.EventAcquirerApproved`, `transaction.EventStartDispense`, `transaction.EventPumpStopped`, `transaction.EventCapture`, `transaction.EventAcquirerCaptured`, `transaction.StatusCaptured` (Phase 1 contracts).
  - `store.Store` (`Pool *pgxpool.Pool`); `pgx`/`pgxpool`; `uuid.UUID`; the `newAuthorizer(t)` helper, `service.AuthorizeRequest`, `(*Authorizer).Authorize` (Phase 2 `service_test`).
- Produces:
  - `const ( AccountCashClearing = "cash-clearing"; AccountFuelRevenue = "fuel-revenue" )` in `internal/service`.
  - `type Capturer struct { Store *store.Store }` and `func NewCapturer(s *store.Store) *Capturer`.
  - `func (c *Capturer) Capture(ctx context.Context, id uuid.UUID, expectedVersion int64, captured money.Amount) (*transaction.Transaction, error)` — opens ONE `pgx.Tx` and within it: (1) `Store.ApplyTransitionTx(... EventCapture ...)` `COMPLETED → CAPTURING`, (2) `Store.ApplyTransitionTx(... EventAcquirerCaptured ...)` `CAPTURING → CAPTURED` stamping `CapturedAmount = captured`, (3) `PostEntries` a balanced pair `DEBIT cash-clearing / CREDIT fuel-revenue` for `captured`, then COMMIT. The `captured <= AuthAmount` HARD guard is enforced BEFORE stamping (after loading the row at CAPTURING): if `captured > AuthAmount` it returns `store.ErrCaptureExceedsAuth` and posts NOTHING — the tx is rolled back, status unchanged. Both the state change and the ledger post commit atomically (ADR-002). The transaction reaches `CAPTURED` and the global fold shows `debits == credits` with no in-flight window.

- [ ] **Step 1: Write the failing test**

Three tests in `internal/service/capture_test.go`. The first drives the transaction to `COMPLETED` through the REAL state machine via the store's version-CAS `ApplyTransition` (`Authorize → AcquirerApproved → StartDispense → PumpStopped`) — never a raw `UPDATE ... SET status` — then captures and asserts the invariant. The second proves the HARD guard: `captured > AuthAmount` returns `ErrCaptureExceedsAuth` and posts nothing / leaves status unchanged. The third proves the BOUNDARY: `captured == AuthAmount` is allowed.

```go
package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/service"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// driveToCompleted walks a freshly-authorized transaction to COMPLETED through
// the REAL state machine via the store's version-CAS ApplyTransition, exercising
// the true transition path (no raw UPDATE ... SET status). It returns the id and
// the version at COMPLETED.
func driveToCompleted(ctx context.Context, t *testing.T, a *service.Authorizer, s *store.Store, key, pump string, auth money.Amount) (uuid.UUID, int64) {
	t.Helper()
	id, err := a.Authorize(ctx, service.AuthorizeRequest{
		IdempotencyKey: key,
		RequestHash:    "hash-v1",
		PumpID:         pump,
		FuelGrade:      "RON95",
		AuthAmount:     auth,
	})
	if err != nil {
		t.Fatalf("seed authorize: %v", err)
	}

	// Authorize already created the row at AUTHORIZING (version 1) via the real
	// authorize path. Walk it forward through the genuine transition table:
	//   AUTHORIZING --AcquirerApproved--> AUTHORIZED
	//   AUTHORIZED  --StartDispense-----> DISPENSING
	//   DISPENSING  --PumpStopped-------> COMPLETED
	version := int64(1)
	for _, e := range []transaction.Event{
		transaction.EventAcquirerApproved,
		transaction.EventStartDispense,
		transaction.EventPumpStopped,
	} {
		tr, err := s.ApplyTransition(ctx, id, version, e, nil)
		if err != nil {
			t.Fatalf("ApplyTransition(%s): %v", e, err)
		}
		version = tr.Version
	}
	return id, version
}

func TestCapture_PostsStateAndLedgerInOneTx_InvariantHolds(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)
	cap := service.NewCapturer(s)

	// Drive to COMPLETED through the REAL state machine. AuthAmount RM200.00.
	id, version := driveToCompleted(ctx, t, a, s, "pump-5-capture-seed", "pump-5", money.Amount(20000))

	// The actual amount dispensed is LOWER than the pre-auth (capture the lower actual).
	captured := money.Amount(17500) // RM175.00

	// THE DEMONSTRATION: one Capture call does COMPLETED->CAPTURING->CAPTURED AND
	// the ledger post in one tx, reusing the store's ApplyTransitionTx version-CAS.
	got, err := cap.Capture(ctx, id, version, captured)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if got.Status != transaction.StatusCaptured {
		t.Fatalf("status = %q, want CAPTURED", got.Status)
	}
	if got.CapturedAmount != captured {
		t.Fatalf("captured_amount = %d, want %d", int64(got.CapturedAmount), int64(captured))
	}

	// Persisted: the row is CAPTURED.
	var status string
	if err := s.Pool.QueryRow(ctx,
		`SELECT status FROM transactions WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if status != "CAPTURED" {
		t.Fatalf("persisted status = %q, want CAPTURED", status)
	}

	// THE INVARIANT (ADR-002): after the capture, the global fold balances.
	debits, credits, err := s.FoldInvariant(ctx)
	if err != nil {
		t.Fatalf("FoldInvariant: %v", err)
	}
	if debits != credits {
		t.Fatalf("invariant broken after capture: debits=%d credits=%d", int64(debits), int64(credits))
	}
	if debits != captured {
		t.Fatalf("debits = %d, want the captured amount %d", int64(debits), int64(captured))
	}

	// Exactly one balanced pair of ledger entries was posted for this txn.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 ledger entries after capture, got %d", n)
	}
}

func TestCapture_ExceedsAuth_RejectedAndPostsNothing(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)
	cap := service.NewCapturer(s)

	// AuthAmount RM150.00; attempt to capture RM150.01 (one sen over) — a HARD
	// guard violation (captured > auth) per spec §8 / ADR money rules.
	id, version := driveToCompleted(ctx, t, a, s, "pump-7-over-capture", "pump-7", money.Amount(15000))

	_, err := cap.Capture(ctx, id, version, money.Amount(15001))
	if !errors.Is(err, store.ErrCaptureExceedsAuth) {
		t.Fatalf("Capture(over-auth): want ErrCaptureExceedsAuth, got %v", err)
	}

	// Status UNCHANGED: still COMPLETED (the transition was rolled back).
	var status string
	var gotVersion int64
	if err := s.Pool.QueryRow(ctx,
		`SELECT status, version FROM transactions WHERE id = $1`, id).Scan(&status, &gotVersion); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if status != "COMPLETED" {
		t.Fatalf("status = %q, want COMPLETED (unchanged after rejected capture)", status)
	}
	if gotVersion != version {
		t.Fatalf("version = %d, want %d (unchanged after rejected capture)", gotVersion, version)
	}

	// NOTHING posted to the ledger for this txn.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 ledger entries after a rejected over-auth capture, got %d", n)
	}
}

func TestCapture_EqualToAuth_Allowed(t *testing.T) {
	ctx := context.Background()
	a, s := newAuthorizer(t)
	cap := service.NewCapturer(s)

	// BOUNDARY: captured == auth is allowed (the guard is captured <= auth).
	auth := money.Amount(15000)
	id, version := driveToCompleted(ctx, t, a, s, "pump-8-equal-capture", "pump-8", auth)

	got, err := cap.Capture(ctx, id, version, auth)
	if err != nil {
		t.Fatalf("Capture(captured==auth): want success, got %v", err)
	}
	if got.Status != transaction.StatusCaptured {
		t.Fatalf("status = %q, want CAPTURED", got.Status)
	}
	if got.CapturedAmount != auth {
		t.Fatalf("captured_amount = %d, want %d", int64(got.CapturedAmount), int64(auth))
	}

	// The balanced pair was posted at exactly the auth amount.
	var n int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 ledger entries after boundary capture, got %d", n)
	}
}
```
- [ ] **Step 2: Run the test, watch it fail**
Run: `go test -race ./internal/service/... -run TestCapture_ -v`
Expected: FAIL to compile — `undefined: service.NewCapturer` and `undefined: service.Capturer`.
- [ ] **Step 3: Implement**

`internal/service/capture.go`:
```go
package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/marwanbukhori/go-brainstorming/internal/ledger"
	"github.com/marwanbukhori/go-brainstorming/internal/money"
	"github.com/marwanbukhori/go-brainstorming/internal/store"
	"github.com/marwanbukhori/go-brainstorming/internal/transaction"
)

// Ledger accounts for the fuel-capture posting. Simple string constants — no
// sharding, no clearing-sweep (those are later plans).
const (
	AccountCashClearing = "cash-clearing"
	AccountFuelRevenue  = "fuel-revenue"
)

// Capturer turns a COMPLETED fuel transaction into a CAPTURED one and posts the
// balanced ledger entries — both in ONE Postgres transaction (ADR-002).
type Capturer struct {
	Store *store.Store
}

// NewCapturer constructs a Capturer.
func NewCapturer(s *store.Store) *Capturer {
	return &Capturer{Store: s}
}

// Capture is the headline ADR-002 demonstration: a single pgx.Tx performs BOTH
// the state transition (COMPLETED -> CAPTURING -> CAPTURED) AND the balanced
// ledger posting (DEBIT cash-clearing / CREDIT fuel-revenue for the captured
// amount). The state transition reuses the store's version-CAS core
// ApplyTransitionTx inside the shared tx — there is ONE version-CAS
// implementation, never a copy. Because both commit atomically, the global
// invariant debits==credits is continuously true — no captured-but-unbooked
// window.
//
// The captured <= auth_amount HARD guard (spec §8 / ADR money rules) is enforced
// here, at the service layer, BEFORE stamping the captured amount: if
// captured > AuthAmount it returns store.ErrCaptureExceedsAuth and posts NOTHING
// — the tx is rolled back and the row's status is left unchanged. The pure state
// machine does not carry amount guards.
//
// expectedVersion is the version of the row at COMPLETED (the caller holds it
// from the prior transition). captured is the actual dispensed amount, which is
// typically lower than the pre-auth.
func (c *Capturer) Capture(ctx context.Context, id uuid.UUID, expectedVersion int64, captured money.Amount) (*transaction.Transaction, error) {
	if captured <= 0 {
		return nil, fmt.Errorf("capture amount must be > 0, got %d", int64(captured))
	}

	tx, err := c.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin capture tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// (1) COMPLETED -> CAPTURING, reusing the store's version-CAS core in this tx.
	t, err := c.Store.ApplyTransitionTx(ctx, tx, id, expectedVersion, transaction.EventCapture, nil)
	if err != nil {
		return nil, fmt.Errorf("transition to CAPTURING: %w", err)
	}

	// HARD GUARD (captured <= auth_amount): enforced at the service layer, before
	// stamping. On violation we return ErrCaptureExceedsAuth and post nothing —
	// the deferred Rollback leaves the row's status unchanged.
	if captured > t.AuthAmount {
		return nil, store.ErrCaptureExceedsAuth
	}

	// (2) CAPTURING -> CAPTURED, stamping the (guarded) captured amount.
	t, err = c.Store.ApplyTransitionTx(ctx, tx, id, t.Version, transaction.EventAcquirerCaptured, func(tr *transaction.Transaction) {
		tr.CapturedAmount = captured
	})
	if err != nil {
		return nil, fmt.Errorf("transition to CAPTURED: %w", err)
	}

	// (3) Post the balanced ledger pair in the SAME tx.
	entries := []ledger.Entry{
		{TransactionID: id, Account: AccountCashClearing, Direction: ledger.Debit, Amount: captured},
		{TransactionID: id, Account: AccountFuelRevenue, Direction: ledger.Credit, Amount: captured},
	}
	if err := c.Store.PostEntries(ctx, tx, entries); err != nil {
		return nil, fmt.Errorf("post capture ledger entries: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit capture: %w", err)
	}
	return t, nil
}
```

Note: the guard reads `t.AuthAmount` from the row loaded by `ApplyTransitionTx` (which loads ALL columns including `auth_amount_minor`), so no extra query is needed. The guard sits AFTER the `COMPLETED -> CAPTURING` transition but BEFORE the `CAPTURED` stamp; on violation the deferred `tx.Rollback(ctx)` undoes the `CAPTURING` transition too, so the persisted status stays `COMPLETED` and nothing is posted. There is exactly ONE version-CAS implementation in the codebase — the store's `ApplyTransitionTx` — reused here in the shared capture tx; this file adds no duplicated transition helper, no exported `ApplyTransitionTxForTest` shim, and no `var _` compile anchor.
- [ ] **Step 4: Run the test, watch it pass**
Run: `go test -race ./internal/service/... -run TestCapture_ -v`
Expected: PASS — the happy path reaches `CAPTURED` at the captured amount with two ledger rows and `FoldInvariant` `debits == credits == 17500`; the over-auth case returns `ErrCaptureExceedsAuth` with status still `COMPLETED` and 0 ledger rows; the boundary case (`captured == auth`) succeeds. Then run the whole Phase 3 suite to confirm no regression:
Run: `go test -race ./internal/money/... ./internal/transaction/... ./internal/ledger/... ./internal/store/... ./internal/service/... -v`
Expected: PASS (all unit + integration tests across the money core).
- [ ] **Step 5: Commit**
```bash
git add internal/service/capture.go internal/service/capture_test.go
git commit -m "feat: capture posts state + ledger in ONE tx (ADR-002 demonstration)

One pgx.Tx does COMPLETED->CAPTURING->CAPTURED AND a balanced DEBIT cash-clearing
/ CREDIT fuel-revenue post for the captured amount, reusing the store's
ApplyTransitionTx version-CAS core (no duplicated transition helper, no test shim,
no compile anchor). Enforces the captured<=auth HARD guard (ErrCaptureExceedsAuth)
before stamping: on violation it posts nothing and leaves status unchanged. The
demonstration test drives to COMPLETED through the real state machine via
ApplyTransition (Authorize->AcquirerApproved->StartDispense->PumpStopped), then
captures; after a valid capture the txn is CAPTURED and FoldInvariant shows
debits==credits — no captured-but-unbooked window. Boundary captured==auth allowed.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Execution Handoff

Plan complete. To implement, use **superpowers:subagent-driven-development** (recommended — a fresh subagent per task with two-stage review) or **superpowers:executing-plans** (batch execution with checkpoints). Start at Phase 0 Task 1. Prerequisite: Docker running (for the testcontainers integration tests). Each task is an independently committable red→green→commit increment; do not skip the `-race` runs.

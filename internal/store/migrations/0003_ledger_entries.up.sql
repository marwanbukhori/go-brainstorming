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

CREATE TABLE idempotency_keys (
    key               text        PRIMARY KEY,           -- the UNIQUE constraint IS the dedupe mechanism (ADR-001)
    request_hash      text        NOT NULL,
    transaction_id    uuid        NOT NULL,
    response_snapshot jsonb,
    created_at        timestamptz NOT NULL DEFAULT now()
);

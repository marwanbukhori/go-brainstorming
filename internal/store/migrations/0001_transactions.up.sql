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

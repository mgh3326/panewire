CREATE TABLE account_credit_marks (
    account_key TEXT PRIMARY KEY,
    pool TEXT NOT NULL,
    marked_ms BIGINT NOT NULL
);

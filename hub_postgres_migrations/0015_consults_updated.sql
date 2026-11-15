-- consult_requests tracks its last write so the coordinator can tell a row a
-- request is about to claim from one a crash left behind (hk 4552).
ALTER TABLE consult_requests ADD COLUMN updated_ms BIGINT NOT NULL DEFAULT 0;

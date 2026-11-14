-- The durable placement snapshot each bound attempt leaves so the hk
-- placement record can be built after the placing App is gone (hk 4615):
-- payload is the latest candidate trace, recorded marks the attempt whose
-- record was already produced.
CREATE TABLE IF NOT EXISTS spawn_placement_snapshots (request_id TEXT NOT NULL,attempt INTEGER NOT NULL,payload TEXT NOT NULL,recorded INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(request_id,attempt));

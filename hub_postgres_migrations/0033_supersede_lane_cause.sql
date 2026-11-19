-- Preserve MySQL's lock-row keys across a copy-back. PostgreSQL continues to
-- use transaction advisory locks; these rows are only used by the copier.
CREATE TABLE IF NOT EXISTS supersede_lane_cause (
    lock_key TEXT PRIMARY KEY
);

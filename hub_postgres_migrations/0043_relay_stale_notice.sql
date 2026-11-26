-- hk 6924: the daily stale-replay notice. One row per real message the replay
-- gate retired as stale, kept until its design lane's notice for the UTC day is
-- filed (filed=1), then pruned. The SQLite twin is relayStaleNoticeSchema; the
-- MySQL twin is migration 0039.
CREATE TABLE IF NOT EXISTS relay_stale_notice_rows (
    relay_id BIGINT PRIMARY KEY,
    notice_day VARCHAR(32) NOT NULL,
    design_lane VARCHAR(255) NOT NULL,
    worker_lane VARCHAR(255) NOT NULL,
    job VARCHAR(255) NOT NULL DEFAULT '',
    task_id VARCHAR(32) NOT NULL DEFAULT '',
    kind VARCHAR(64) NOT NULL,
    sender VARCHAR(32) NOT NULL DEFAULT '',
    hk_event_id VARCHAR(255) NOT NULL DEFAULT '',
    received_ms BIGINT NOT NULL,
    retired_ms BIGINT NOT NULL,
    filed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS relay_stale_notice_rows_day ON relay_stale_notice_rows(notice_day, design_lane, filed);

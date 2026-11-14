-- Each design lane's owed drain line and last send (hub_drain_wake.go, hk
-- 4567). A hub restart keeps a line that is still owed, as the SQLite store does.
CREATE TABLE IF NOT EXISTS drain_wake_lanes (
  lane TEXT PRIMARY KEY,
  pending INTEGER NOT NULL DEFAULT 0,
  last_sent BIGINT NOT NULL DEFAULT 0
);

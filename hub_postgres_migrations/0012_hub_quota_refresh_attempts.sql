-- Every finished refresh attempt, success included (outcome ''), so a timeout
-- that expires while a request waits can tell a failed measurement from one
-- that finished and from one that never ran (hk 4600, follow-up to 0011).
-- 0011 is already applied on hubs that ran it, so the table arrives here and
-- fresh databases get it through this file as well. quota_refresh_failures
-- from 0011 stays for the old binary during a rolling deploy; this build only
-- reads and writes quota_refresh_attempts.
CREATE TABLE IF NOT EXISTS quota_refresh_attempts (machine TEXT NOT NULL,pool TEXT NOT NULL,outcome TEXT NOT NULL,seen_ms BIGINT NOT NULL,PRIMARY KEY(machine,pool));

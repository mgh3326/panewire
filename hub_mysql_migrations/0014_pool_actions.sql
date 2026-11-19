-- Pool actions. A final row (done or refused) is the dedupe fence for its job, and
-- final_ms is when it became final (phase 0 retention, hk 5297).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='pool_actions'
CREATE TABLE pool_actions (
 id VARCHAR(255) NOT NULL,
 job VARCHAR(255) NOT NULL,
 node VARCHAR(255) NOT NULL,
 epoch BIGINT NOT NULL,
 phase VARCHAR(32) NOT NULL,
 data MEDIUMTEXT NOT NULL,
 final_ms BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (id),
 KEY pool_actions_job (job, phase),
 KEY pool_actions_phase (phase, id),
 KEY pool_actions_final (phase, final_ms),
 CHECK (epoch>0),
 CHECK (phase IN ('pending','guarded','settled','done','refused'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

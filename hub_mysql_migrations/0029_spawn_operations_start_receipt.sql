-- hk 5776: start_receipt_ms is when the node an attempt is bound to reported that its
-- execution landed (job.spawn.started, sent from the durable landing point of the
-- spawn). It is the start proof a quota-stop replacement needs before it writes
-- replaced_by; a job-name heartbeat (claimed_ms) names no request and is not one.
-- A bind clears it.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='spawn_operations' AND column_name='start_receipt_ms'
ALTER TABLE spawn_operations ADD COLUMN start_receipt_ms BIGINT NOT NULL DEFAULT 0;

-- Read-on-insert interrupts are not confirmation that a pane received them.
ALTER TABLE lane_inbox ADD COLUMN delivered_at BIGINT NOT NULL DEFAULT 0;

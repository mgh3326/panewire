-- A consult row carries the lane that owns it (operator decision 421, hk 5233):
-- the author job's owner lane, else the requester lane, else operator-desk.
-- A row from before the column reads the requester rule.
ALTER TABLE consult_requests ADD COLUMN owner TEXT NOT NULL DEFAULT '';

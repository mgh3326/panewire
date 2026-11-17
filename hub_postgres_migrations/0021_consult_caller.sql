-- Retain the node and pane that originated a consult request.
ALTER TABLE consult_requests ADD COLUMN machine TEXT NOT NULL DEFAULT '';
ALTER TABLE consult_requests ADD COLUMN caller_pane TEXT NOT NULL DEFAULT '';

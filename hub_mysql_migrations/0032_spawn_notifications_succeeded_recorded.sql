-- hk 6283: a succeeded outcome owes no failure event, so its spawn notice
-- counts as telemetry-recorded. The hub writes new ones 1
-- (spawnNoticeRecordedAtInsert); this marks the rows written before, once.
-- A row's own outcome is the status segment of its event id,
-- <request_id>:<attempt>:<status>, with :<owner> appended on a submitter copy.
-- A lane name may hold ':' and may be "succeeded", so an owner row is matched
-- by its segment count and a copy by the owner it ends in. Rows of every other
-- status stay as they are, even when their operation later succeeded.
-- The statement runs again, each run its own commit, until one matches fewer
-- rows than a batch. It takes only unmarked rows, so a repeat of the file is a
-- no-op and the guard never skips it.
-- applied-if: SELECT 0
-- batch-rows: 1000
UPDATE spawn_notifications SET telemetry_recorded=1
WHERE telemetry_recorded=0
 AND ((event_id LIKE CONCAT(request_id,':%:succeeded') AND event_id NOT LIKE CONCAT(request_id,':%:%:succeeded'))
  OR event_id LIKE CONCAT(request_id,':%:succeeded:',owner))
LIMIT 1000;

-- hk 6283: a succeeded outcome owes no failure event, so its spawn notice
-- counts as telemetry-recorded. The hub writes new ones 1
-- (spawnNoticeRecordedAtInsert); this marks the rows written before, once.
-- A row's own outcome is the status segment of its event id,
-- <request_id>:<attempt>:<status>, with :<owner> appended on a submitter copy.
-- A lane name may hold ':' and may be "succeeded", so an owner row is matched
-- by its segment count and a copy by the owner it ends in. Rows of every other
-- status stay as they are, even when their operation later succeeded.
-- One statement: the hub logs the rows this file updates.
-- MySQL's twin is migration 0032; SQLite's is backfillSucceededSpawnNotices.
UPDATE spawn_notifications SET telemetry_recorded=1
WHERE telemetry_recorded=0
 AND ((event_id LIKE request_id||':%:succeeded' AND event_id NOT LIKE request_id||':%:%:succeeded')
  OR event_id LIKE request_id||':%:succeeded:'||owner);

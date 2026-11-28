-- hk 7125: open_dispatches and queued_spawns are the node's queue as an
-- attempt's first scheduling pass counted it: the in-flight dispatches on the
-- machine, and the other operations on it in flight or waiting ahead. They are
-- stored on the operation so a landing recorded after the dispatch has ended
-- still carries them. -1 means no pass recorded them. Additive only: an older
-- hub neither reads nor writes the columns.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS open_dispatches INTEGER NOT NULL DEFAULT -1;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS queued_spawns INTEGER NOT NULL DEFAULT -1;

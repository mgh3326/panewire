-- hk 7125: open_dispatches and queued_spawns are the node's queue as an
-- attempt's first scheduling pass counted it: the in-flight dispatches on the
-- machine, and the other operations on it in flight or waiting ahead. They are
-- stored on the operation so a landing recorded after the dispatch has ended
-- still carries them. -1 means no pass recorded them.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='spawn_operations' AND column_name='open_dispatches'
ALTER TABLE spawn_operations ADD COLUMN open_dispatches INT NOT NULL DEFAULT -1, ADD COLUMN queued_spawns INT NOT NULL DEFAULT -1;

-- hk 5690: replaced_reason says who wrote replaced_from. The claim of a quota-stop
-- or hand-over successor writes 'claim'; a row whose replaced_from the startup
-- backfill derived from the job's "-q<n>" suffix keeps ''. A derived lineage is no
-- proof that a job replaced anything, so the quota replacement requires the claim's
-- own marker. Additive only: an older hub neither reads nor writes the column.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS replaced_reason TEXT NOT NULL DEFAULT '';

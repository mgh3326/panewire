-- hk 5690: replaced_reason says who wrote replaced_from. The claim of a quota-stop
-- or hand-over successor writes 'claim'; a row whose replaced_from the startup
-- backfill derived from the job's "-q<n>" suffix keeps ''. A derived lineage is no
-- proof that a job replaced anything.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='spawn_operations' AND column_name='replaced_reason'
ALTER TABLE spawn_operations ADD COLUMN replaced_reason VARCHAR(32) NOT NULL DEFAULT '';

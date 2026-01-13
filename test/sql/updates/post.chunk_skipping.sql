-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Show chunk column stats after updating the extension. We need to
-- exclude the rows with chunk_id = 0 because we cannot keep those on
-- downgrade due to FK constraint. Showing them would mean a diff in
-- the output. We can still test that 0 chunk_ids are converted to
-- NULL values during upgrades, however.
SELECT * FROM _timescaledb_catalog.chunk_column_stats
WHERE chunk_id IS NULL OR chunk_id > 0 ORDER BY id;

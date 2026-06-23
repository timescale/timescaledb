-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Show chunk column stats after updating the extension. We need to
-- exclude the rows with chunk_id = 0 because we cannot keep those on
-- downgrade due to FK constraint. Showing them would mean a diff in
-- the output. We can still test that 0 chunk_ids are converted to
-- NULL values during upgrades, however. The chunk id itself is renumbered
-- across the upgrade, so report only whether it is set instead of its value.
SELECT id, hypertable_id, (chunk_id IS NOT NULL) AS chunk_id_set, column_name, range_start, range_end, valid
FROM _timescaledb_catalog.chunk_column_stats
WHERE chunk_id IS NULL OR chunk_id > 0 ORDER BY id;

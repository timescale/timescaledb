-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Show chunk column stats after updating the extension. We need to
-- exclude the rows with chunk_id = 0 because we cannot keep those on
-- downgrade due to FK constraint. Showing them would mean a diff in
-- the output. We can still test that 0 chunk_ids are converted to
-- NULL values during upgrades, however.
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid::regclass::text;

SELECT
    schema_name || '.' || table_name AS chunk
FROM _timescaledb_catalog.chunk
WHERE id = (
    SELECT compressed_chunk_id
    FROM _timescaledb_catalog.chunk
    WHERE hypertable_id = (
        SELECT id
        FROM _timescaledb_catalog.hypertable
        WHERE table_name = 'bloom'
    )
    LIMIT 1
)
\gset

-- Note: we can't use \d+ here because it prevents changing the TOAST storage flag
--   if we do, like in the UUID compression changes from external to extended, then
--   the upgrade tests fail
\d :chunk


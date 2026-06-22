-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Show chunk column stats after updating the extension. We need to
-- exclude the rows with chunk_id = 0 because we cannot keep those on
-- downgrade due to FK constraint. Showing them would mean a diff in
-- the output. We can still test that 0 chunk_ids are converted to
-- NULL values during upgrades, however.

SELECT
    cs.compress_relid::text AS chunk
FROM _timescaledb_catalog.chunk ch
JOIN _timescaledb_catalog.compression_settings cs
    ON cs.relid = format('%I.%I', ch.schema_name, ch.table_name)::regclass
WHERE ch.hypertable_id = (
    SELECT id
    FROM _timescaledb_catalog.hypertable
    WHERE table_name = 'bloom'
)
LIMIT 1
\gset

-- This test checks that the bloom sparse indexes survive the upgrade, so only
-- look at the bloom columns of the compressed chunk. Dumping the whole chunk
-- would also pull in the orderby sparse metadata, whose layout depends on the
-- version that compressed the chunk (minmax vs firstlast) and is not rewritten
-- on upgrade, which would cause a spurious diff unrelated to bloom indexes.
\a
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
WHERE a.attrelid = :'chunk'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND t.typname = 'bloom1'
ORDER BY a.attnum;
\a


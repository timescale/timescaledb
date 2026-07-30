-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- The 2.26.4--2.27.0 migration must drop the incompatible smallint bloom filter
-- columns together with their compression settings. Each bloom sparse index in
-- the settings owns exactly one bloom1 metadata column on the compressed chunk,
-- so the two counts must match on every compressed chunk. This holds both for a
-- fresh install (bloom kept) and for a migrated database (bloom dropped); a
-- leftover settings entry or an orphaned column would make the counts differ and
-- show up here as a diff.
\a
WITH bloom_entry_counts AS (
  SELECT cs.compress_relid, count(*) AS n
  FROM _timescaledb_catalog.compression_settings cs,
       jsonb_array_elements(cs.index) AS elem
  WHERE cs.compress_relid IS NOT NULL
    AND cs.index IS NOT NULL
    AND elem->>'type' = 'bloom'
  GROUP BY cs.compress_relid
),
bloom_column_counts AS (
  SELECT cs.compress_relid, count(*) AS n
  FROM _timescaledb_catalog.compression_settings cs
  JOIN pg_attribute a ON a.attrelid = cs.compress_relid
   AND a.attnum > 0
   AND NOT a.attisdropped
  JOIN pg_type t ON t.oid = a.atttypid AND t.typname = 'bloom1'
  WHERE cs.compress_relid IS NOT NULL
  GROUP BY cs.compress_relid
)
SELECT coalesce(e.n, 0) AS bloom_settings_entries,
       coalesce(c.n, 0) AS bloom_columns
FROM bloom_entry_counts e
FULL JOIN bloom_column_counts c ON e.compress_relid = c.compress_relid
WHERE coalesce(e.n, 0) <> coalesce(c.n, 0)
ORDER BY 1, 2;
\a

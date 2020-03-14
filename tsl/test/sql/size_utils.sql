-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE compressed(time timestamptz NOT NULL);
SELECT create_hypertable('compressed','time');

-- test that empty hypertable is still included in result
SELECT * FROM hypertable_approximate_row_count('compressed');

-- create 5 chunks
INSERT INTO compressed SELECT generate_series('2000-01-01'::timestamptz, '2000-02-01'::timestamptz, '1d'::interval);

VACUUM compressed;

-- row count should be 32
SELECT * FROM hypertable_approximate_row_count('compressed');

SELECT * FROM hypertable_relation_size('compressed');
SELECT * FROM hypertable_relation_size_pretty('compressed');

-- compress first 2 chunks
ALTER TABLE compressed SET (timescaledb.compress);
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable ht ON c.hypertable_id = ht.id
WHERE ht.table_name like 'compressed' ORDER BY c.id limit 2;

VACUUM compressed;

-- row count should be 32
SELECT * FROM hypertable_approximate_row_count('compressed');

SELECT * FROM hypertable_relation_size('compressed');
SELECT * FROM hypertable_relation_size_pretty('compressed');


-- Restore chunk_constraint rows for CHECK constraints on OSM chunks.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, con.conname, con.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint con
    ON con.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND con.contype = 'c'
WHERE c.osm_chunk
ON CONFLICT DO NOTHING;

-- Drop chunk stats related objects
DROP VIEW IF EXISTS timescaledb_information.stat_chunk_activity;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_statistics(regclass, regclass, timestamptz);
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_statistics_reset();

-- Restore chunk_constraint rows for outbound FKs by matching chunk-side
-- FKs to their hypertable-side counterpart by name.
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, child.conname, parent.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint parent
    ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND parent.contype = 'f'
JOIN pg_constraint child
    ON child.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
    AND child.contype = 'f'
    AND child.conname = parent.conname
ON CONFLICT DO NOTHING;

-- Restore chunk_constraint rows for unique/PK/exclusion/trigger constraints.
-- The chunk-side conname is the deterministic "<chunk_id>_<parent>" form
INSERT INTO _timescaledb_catalog.chunk_constraint
    (chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name)
SELECT c.id, NULL, child.conname, parent.conname
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
JOIN pg_constraint parent
    ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
    AND parent.contype IN ('u', 'p', 'x', 't')
JOIN pg_constraint child
    ON child.conrelid = pg_catalog.format('%I.%I', c.schema_name, c.table_name)::regclass
    AND child.conname = format('%s_%s', c.id, parent.conname)
ON CONFLICT DO NOTHING;

ALTER TABLE _timescaledb_catalog.hypertable RESET (user_catalog_table);
ALTER TABLE _timescaledb_catalog.chunk RESET (user_catalog_table);

--
-- Rebuild the catalog table `_timescaledb_catalog.dimension_slice` back to
-- the pre-chunk_id schema. Per-chunk slice duplicates are collapsed back
-- into one shared row per (dimension_id, range_start, range_end), and the
-- old UNIQUE on that triple is restored.
--
CREATE TABLE _timescaledb_internal.tmp_dimension_slice AS
    SELECT * FROM _timescaledb_catalog.dimension_slice;
CREATE TABLE _timescaledb_internal.tmp_dimension_slice_seq_value AS
    SELECT last_value, is_called FROM _timescaledb_catalog.dimension_slice_id_seq;

ALTER TABLE _timescaledb_catalog.chunk_constraint
    DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey;

DROP VIEW IF EXISTS timescaledb_information.chunks;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_slice;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.dimension_slice_id_seq;

DROP TABLE _timescaledb_catalog.dimension_slice;

CREATE TABLE _timescaledb_catalog.dimension_slice (
  id serial NOT NULL,
  dimension_id integer NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  CONSTRAINT dimension_slice_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_slice_dimension_id_range_start_range_end_key UNIQUE (dimension_id, range_start, range_end),
  CONSTRAINT dimension_slice_check CHECK (range_start <= range_end),
  CONSTRAINT dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id) REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE
);

-- One row per unique (dimension_id, range_start, range_end), reusing the
-- lowest old id so existing chunk_constraint rows that already point at
-- it don't need repointing.
INSERT INTO _timescaledb_catalog.dimension_slice (id, dimension_id, range_start, range_end)
SELECT min(id), dimension_id, range_start, range_end
FROM _timescaledb_internal.tmp_dimension_slice
GROUP BY dimension_id, range_start, range_end;

-- Repoint chunk_constraint rows that referenced one of the deduplicated
-- (now deleted) slice ids at the kept slice with the same range.
UPDATE _timescaledb_catalog.chunk_constraint cc
SET dimension_slice_id = ds.id
FROM _timescaledb_internal.tmp_dimension_slice tmp,
     _timescaledb_catalog.dimension_slice ds
WHERE cc.dimension_slice_id IS NOT NULL
  AND tmp.id = cc.dimension_slice_id
  AND tmp.id <> ds.id
  AND ds.dimension_id = tmp.dimension_id
  AND ds.range_start = tmp.range_start
  AND ds.range_end = tmp.range_end;

ALTER SEQUENCE _timescaledb_catalog.dimension_slice_id_seq OWNED BY _timescaledb_catalog.dimension_slice.id;
SELECT setval('_timescaledb_catalog.dimension_slice_id_seq', last_value, is_called)
    FROM _timescaledb_internal.tmp_dimension_slice_seq_value;

ALTER TABLE _timescaledb_catalog.chunk_constraint
    ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
        FOREIGN KEY (dimension_slice_id) REFERENCES _timescaledb_catalog.dimension_slice (id);

DROP TABLE _timescaledb_internal.tmp_dimension_slice;
DROP TABLE _timescaledb_internal.tmp_dimension_slice_seq_value;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice', 'id'), '');

GRANT SELECT ON _timescaledb_catalog.dimension_slice TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.dimension_slice_id_seq TO PUBLIC;
-- end rebuild _timescaledb_catalog.dimension_slice table --


DROP FUNCTION IF EXISTS public.time_bucket(INTERVAL, TIMESTAMP);
DROP FUNCTION IF EXISTS public.time_bucket(INTERVAL, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS public.time_bucket(INTERVAL, DATE);
DROP FUNCTION IF EXISTS public.time_bucket(INTERVAL, TIMESTAMP, INTERVAL);
DROP FUNCTION IF EXISTS public.time_bucket(INTERVAL, TIMESTAMPTZ, INTERVAL);
DROP FUNCTION IF EXISTS public.time_bucket(INTERVAL, DATE, INTERVAL);
DROP FUNCTION IF EXISTS public.time_bucket(BIGINT, BIGINT);
DROP FUNCTION IF EXISTS public.time_bucket(INT, INT);
DROP FUNCTION IF EXISTS public.time_bucket(SMALLINT, SMALLINT);
DROP FUNCTION IF EXISTS public.time_bucket(BIGINT, BIGINT, BIGINT);
DROP FUNCTION IF EXISTS public.time_bucket(INT, INT, INT);
DROP FUNCTION IF EXISTS public.time_bucket(SMALLINT, SMALLINT, SMALLINT);

-- Indexing updates
DROP EVENT TRIGGER IF EXISTS ddl_create_index;
DROP EVENT TRIGGER IF EXISTS ddl_alter_index;
DROP EVENT TRIGGER IF EXISTS ddl_drop_index;
DROP TRIGGER IF EXISTS trigger_main_on_change_chunk_index ON _timescaledb_catalog.chunk_index;
DROP TRIGGER IF EXISTS trigger_main_on_change_hypertable_index ON _timescaledb_catalog.hypertable_index;

DROP FUNCTION IF EXISTS _timescaledb_internal.get_index_definition_for_table(NAME, NAME, NAME, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_index_row(NAME, NAME, NAME, NAME, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_internal.on_change_chunk_index();
DROP FUNCTION IF EXISTS _timescaledb_internal.add_index(INTEGER, NAME, NAME, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_index(NAME, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.get_general_index_definition(REGCLASS, REGCLASS, _timescaledb_catalog.hypertable);
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_create_index();
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_alter_index();
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_drop_index();
DROP FUNCTION IF EXISTS _timescaledb_internal.create_index_on_all_chunks(INTEGER, NAME, NAME, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_index_on_all_chunks(NAME, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.on_change_hypertable_index();
DROP FUNCTION IF EXISTS _timescaledb_internal.need_chunk_index(INTEGER, OID);
DROP FUNCTION IF EXISTS _timescaledb_internal.check_index(REGCLASS, _timescaledb_catalog.hypertable);

DROP FUNCTION IF EXISTS indexes_relation_size_pretty(REGCLASS);

ALTER TABLE IF EXISTS _timescaledb_catalog.chunk_index RENAME TO chunk_index_old;
ALTER SEQUENCE IF EXISTS _timescaledb_catalog.chunk_index_id_seq RENAME TO chunk_index_old_id_seq;

-- Create new table
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_index (
    chunk_id              INTEGER NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    index_name            NAME NOT NULL,
    hypertable_id         INTEGER NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    hypertable_index_name NAME NOT NULL,
    UNIQUE(chunk_id, index_name)
);
CREATE INDEX IF NOT EXISTS chunk_index_hypertable_id_hypertable_index_name_idx
ON _timescaledb_catalog.chunk_index(hypertable_id, hypertable_index_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');

-- Remove metadata table triggers
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.hypertable;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.dimension;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.dimension_slice;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.chunk_constraint;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.hypertable_index;
DROP TRIGGER trigger_1_main_on_change_hypertable ON _timescaledb_catalog.hypertable;
DROP FUNCTION IF EXISTS _timescaledb_internal.on_truncate_block();
DROP FUNCTION IF EXISTS _timescaledb_internal.on_trigger_error(TEXT, NAME, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.on_change_hypertable();
DROP FUNCTION IF EXISTS _timescaledb_internal.setup_main(BOOLEAN);
DROP FUNCTION IF EXISTS restore_timescaledb();

DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunk(INTEGER, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS drop_chunks(TIMESTAMPTZ, NAME, NAME);
DROP FUNCTION IF EXISTS drop_chunks(INTERVAL, NAME, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunks_older_than(BIGINT, NAME, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.truncate_hypertable(NAME, NAME);

--- Post script
-- Indexing updates

-- Convert old chunk_index table data to new format
INSERT INTO _timescaledb_catalog.chunk_index
SELECT ch.id, ci.index_name, h.id, ci.main_index_name
FROM _timescaledb_catalog.chunk_index_old ci,
     _timescaledb_catalog.hypertable h,
     _timescaledb_catalog.chunk ch,
     pg_index i,
     pg_class c
WHERE ci.schema_name = ch.schema_name
AND   ci.table_name = ch.table_name
AND   i.indexrelid = format('%I.%I', ci.main_schema_name, ci.main_index_name)::REGCLASS
AND   i.indrelid = c.oid
AND   ci.main_schema_name = h.schema_name
AND   c.relname = h.table_name;

ALTER EXTENSION timescaledb
DROP TABLE _timescaledb_catalog.chunk_index_old;
ALTER EXTENSION timescaledb
DROP TABLE _timescaledb_catalog.hypertable_index;
ALTER EXTENSION timescaledb
DROP SEQUENCE _timescaledb_catalog.chunk_index_old_id_seq;

DROP TABLE IF EXISTS _timescaledb_catalog.chunk_index_old;
DROP TABLE IF EXISTS _timescaledb_catalog.hypertable_index;
-- No need to drop _timescaledb_catalog.chunk_index_old_id_seq,
-- removed with table.

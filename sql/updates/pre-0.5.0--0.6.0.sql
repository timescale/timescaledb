DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMP);
DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMPTZ);
DROP FUNCTION public.time_bucket(INTERVAL, DATE);
DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMP, INTERVAL);
DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMPTZ, INTERVAL);
DROP FUNCTION public.time_bucket(INTERVAL, DATE, INTERVAL);
DROP FUNCTION public.time_bucket(BIGINT, BIGINT);
DROP FUNCTION public.time_bucket(INT, INT);
DROP FUNCTION public.time_bucket(SMALLINT, SMALLINT);
DROP FUNCTION public.time_bucket(BIGINT, BIGINT, BIGINT);
DROP FUNCTION public.time_bucket(INT, INT, INT);
DROP FUNCTION public.time_bucket(SMALLINT, SMALLINT, SMALLINT);

-- Indexing updates
DROP EVENT TRIGGER ddl_create_index;
DROP EVENT TRIGGER ddl_alter_index;
DROP EVENT TRIGGER ddl_drop_index;
DROP TRIGGER trigger_main_on_change_chunk_index ON _timescaledb_catalog.chunk_index;
DROP TRIGGER trigger_main_on_change_hypertable_index ON _timescaledb_catalog.hypertable_index;

DROP FUNCTION _timescaledb_internal.get_index_definition_for_table(NAME, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.create_chunk_index_row(NAME, NAME, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.on_change_chunk_index();
DROP FUNCTION _timescaledb_internal.add_index(INTEGER, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.drop_index(NAME, NAME);
DROP FUNCTION _timescaledb_internal.get_general_index_definition(REGCLASS, REGCLASS, _timescaledb_catalog.hypertable);
DROP FUNCTION _timescaledb_internal.ddl_process_create_index();
DROP FUNCTION _timescaledb_internal.ddl_process_alter_index();
DROP FUNCTION _timescaledb_internal.ddl_process_drop_index();
DROP FUNCTION _timescaledb_internal.create_index_on_all_chunks(INTEGER, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.drop_index_on_all_chunks(NAME, NAME);
DROP FUNCTION _timescaledb_internal.on_change_hypertable_index();
DROP FUNCTION _timescaledb_internal.need_chunk_index(INTEGER, OID);
DROP FUNCTION _timescaledb_internal.check_index(REGCLASS, _timescaledb_catalog.hypertable);

DROP FUNCTION indexes_relation_size_pretty(REGCLASS);

ALTER TABLE _timescaledb_catalog.chunk_index RENAME TO chunk_index_old;
ALTER SEQUENCE _timescaledb_catalog.chunk_index_id_seq RENAME TO chunk_index_old_id_seq;

-- Remove metadata table triggers
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.hypertable;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.dimension;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.dimension_slice;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.chunk_constraint;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.hypertable_index;
DROP TRIGGER trigger_1_main_on_change_hypertable ON _timescaledb_catalog.hypertable;
DROP FUNCTION _timescaledb_internal.on_truncate_block();
DROP FUNCTION _timescaledb_internal.on_trigger_error(TEXT, NAME, NAME);
DROP FUNCTION _timescaledb_internal.on_change_hypertable();
DROP FUNCTION _timescaledb_internal.setup_main(BOOLEAN);
DROP FUNCTION restore_timescaledb();

DROP FUNCTION _timescaledb_internal.drop_chunk(INTEGER, BOOLEAN, BOOLEAN);
DROP FUNCTION drop_chunks(TIMESTAMPTZ, NAME, NAME);
DROP FUNCTION drop_chunks(INTERVAL, NAME, NAME);
DROP FUNCTION _timescaledb_internal.drop_chunks_older_than(BIGINT, NAME, NAME);
DROP FUNCTION _timescaledb_internal.truncate_hypertable(NAME, NAME);

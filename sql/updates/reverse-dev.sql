-- check whether we can safely downgrade the existing compression setup
CREATE OR REPLACE FUNCTION _timescaledb_functions.add_sequence_number_metadata_column(
	comp_ch_schema_name text,
	comp_ch_table_name text
)
	RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	chunk_schema_name text;
	chunk_table_name text;
	index_name text;
	segmentby_columns text;
BEGIN
	SELECT ch.schema_name, ch.table_name INTO STRICT chunk_schema_name, chunk_table_name
	FROM _timescaledb_catalog.chunk ch
	INNER JOIN  _timescaledb_catalog.chunk comp_ch
	ON ch.compressed_chunk_id = comp_ch.id
	WHERE comp_ch.schema_name = comp_ch_schema_name
	AND comp_ch.table_name = comp_ch_table_name;

    IF NOT FOUND THEN
		RAISE USING
			ERRCODE = 'feature_not_supported',
			MESSAGE = 'Cannot migrate compressed chunk to version 2.16.1, chunk not found';
    END IF;

	-- Add sequence number column to compressed chunk
	EXECUTE format('ALTER TABLE %s.%s ADD COLUMN _ts_meta_sequence_num INT DEFAULT NULL', comp_ch_schema_name, comp_ch_table_name);

	-- Remove all indexes from compressed chunk
	FOR index_name IN
		SELECT format('%s.%s', i.schemaname, i.indexname)
		FROM pg_indexes i
		WHERE i.schemaname = comp_ch_schema_name
		AND i.tablename = comp_ch_table_name
	LOOP
		EXECUTE format('DROP INDEX %s;', index_name);
	END LOOP;

	-- Fetch the segmentby columns from compression settings
	SELECT string_agg(cs.segmentby_column, ',') INTO segmentby_columns
	FROM (
		SELECT unnest(segmentby)
		FROM _timescaledb_catalog.compression_settings
		WHERE relid = format('%s.%s', comp_ch_schema_name, comp_ch_table_name)::regclass::oid
		AND segmentby IS NOT NULL
	) AS cs(segmentby_column);

	-- Create compressed chunk index based on sequence num metadata column
	-- If there is no segmentby columns, we can skip creating the index
    IF FOUND AND segmentby_columns IS NOT NULL THEN
		EXECUTE format('CREATE INDEX ON %s.%s (%s, _ts_meta_sequence_num);', comp_ch_schema_name, comp_ch_table_name, segmentby_columns);
    END IF;

	-- Mark compressed chunk as unordered
	-- Marking the chunk status bit (2) makes it unordered
	-- and disables some optimizations. In order to re-enable
	-- them, you need to recompress these chunks.
	UPDATE _timescaledb_catalog.chunk
	SET status = status | 2 -- set unordered bit
	WHERE schema_name = chunk_schema_name
	AND table_name = chunk_table_name;

	RETURN true;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

DO $$
DECLARE
	chunk_count int;
	chunk_record record;
BEGIN
    -- if we find chunks which don't have sequence number metadata column in
	-- compressed chunk, we need to stop downgrade and have the user run
	-- a migration script to re-add the missing columns
	SELECT count(*) INTO STRICT chunk_count
	FROM _timescaledb_catalog.chunk ch
	INNER JOIN _timescaledb_catalog.chunk uncomp_ch
	ON uncomp_ch.compressed_chunk_id = ch.id
	WHERE not exists (
		SELECT
		FROM pg_attribute att
		WHERE attrelid=format('%I.%I',ch.schema_name,ch.table_name)::regclass
		AND attname='_ts_meta_sequence_num')
	AND NOT uncomp_ch.dropped;

	-- Doing the migration if we find 10 or less chunks that need to be migrated
	IF chunk_count > 10 THEN
      RAISE USING
        ERRCODE = 'feature_not_supported',
        MESSAGE = 'Cannot downgrade compressed hypertables with chunks that do not contain sequence numbers. Run timescaledb--2.17-2.16.1.sql migration script before downgrading.',
        DETAIL = 'Number of chunks that need to be migrated: '|| chunk_count::text;
	ELSIF chunk_count > 0 THEN
		FOR chunk_record IN
		SELECT comp_ch.*
		FROM _timescaledb_catalog.chunk ch
		INNER JOIN _timescaledb_catalog.chunk comp_ch
		ON ch.compressed_chunk_id = comp_ch.id
		WHERE not exists (
			SELECT
			FROM pg_attribute att
			WHERE attrelid=format('%I.%I',comp_ch.schema_name,comp_ch.table_name)::regclass
			AND attname='_ts_meta_sequence_num')
			AND NOT ch.dropped
		LOOP
			PERFORM  _timescaledb_functions.add_sequence_number_metadata_column(chunk_record.schema_name, chunk_record.table_name);
			RAISE LOG 'Migrated compressed chunk %s.%s to version 2.16.1', chunk_record.schema_name, chunk_record.table_name;
		END LOOP;

		RAISE LOG 'Migration successful!';
	END IF;
END
$$;

DROP FUNCTION _timescaledb_functions.add_sequence_number_metadata_column(text, text);

DROP FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data);
DROP INDEX _timescaledb_catalog.compression_chunk_size_idx;
DROP FUNCTION IF EXISTS _timescaledb_functions.drop_osm_chunk(REGCLASS);

-- Hyperstore AM
DROP ACCESS METHOD IF EXISTS hsproxy;
DROP FUNCTION IF EXISTS ts_hsproxy_handler;
DROP ACCESS METHOD IF EXISTS hyperstore;
DROP FUNCTION IF EXISTS ts_hyperstore_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN, compress_using NAME);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C STRICT VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL, schedule_interval INTERVAL, initial_start TIMESTAMPTZ, timezone TEXT, compress_created_before INTERVAL, compress_using NAME);

CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies(relation REGCLASS, if_not_exists BOOL, refresh_start_offset "any", refresh_end_offset "any", compress_after "any", drop_after "any", compress_using NAME);

CREATE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL)
RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_policies_add'
LANGUAGE C VOLATILE;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(job_id INTEGER, htid INTEGER, lag ANYELEMENT, maxchunks INTEGER, verbose_log BOOLEAN, recompress_enabled  BOOLEAN, use_creation_time BOOLEAN, amname NAME);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);

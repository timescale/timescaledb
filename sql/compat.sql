-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- TimescaleDB 2.12 moved all functions present in _timescaledb_internal into
-- _timescaledb_functions. This file contains a compatibility layer to allow
-- for more flexibility when migrating for any users calling these internal
-- functions.
-- This compatibility layer will be removed in a future versions.


CREATE OR REPLACE FUNCTION _timescaledb_internal.alter_job_set_hypertable_id(job_id integer, hypertable regclass) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.alter_job_set_hypertable_id(integer,regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.alter_job_set_hypertable_id($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.attach_osm_table_chunk(hypertable regclass, chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.attach_osm_table_chunk(regclass,regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.attach_osm_table_chunk($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_migrate_plan_exists(_hypertable_id integer) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_migrate_plan_exists(integer) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_migrate_plan_exists($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_migrate_pre_validation(_cagg_schema text,_cagg_name text,_cagg_name_new text) RETURNS _timescaledb_catalog.continuous_agg LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_migrate_pre_validation(text,text,text) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_migrate_pre_validation($1,$2,$3);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_watermark(hypertable_id integer) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_watermark(integer) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_watermark($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_watermark_materialized(hypertable_id integer) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_watermark_materialized(integer) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_watermark_materialized($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_chunk_interval(dimension_id integer,dimension_coord bigint,chunk_target_size bigint) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.calculate_chunk_interval(integer,bigint,bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.calculate_chunk_interval($1,$2,$3);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row _timescaledb_catalog.chunk_constraint) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_constraint_add_table_constraint(_timescaledb_catalog.chunk_constraint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.chunk_constraint_add_table_constraint($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_from_relid(relid oid) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_id_from_relid(oid) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.chunk_id_from_relid($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_status(regclass) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_status(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.chunk_status($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_local_size(schema_name_in name,table_name_in name) RETURNS TABLE (chunk_id integer, chunk_schema NAME, chunk_name  NAME, table_bytes bigint, index_bytes bigint, toast_bytes bigint, total_bytes bigint) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunks_local_size(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.chunks_local_size($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_chunk_local_stats(schema_name_in name,table_name_in name) RETURNS TABLE (chunk_schema name, chunk_name name, compression_status text, before_compression_table_bytes bigint, before_compression_index_bytes bigint, before_compression_toast_bytes bigint, before_compression_total_bytes bigint, after_compression_table_bytes bigint, after_compression_index_bytes bigint, after_compression_toast_bytes bigint, after_compression_total_bytes bigint) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.compressed_chunk_local_stats(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.compressed_chunk_local_stats($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_chunk_remote_stats(schema_name_in name,table_name_in name) RETURNS TABLE ( chunk_schema name, chunk_name name, compression_status text, before_compression_table_bytes bigint, before_compression_index_bytes bigint, before_compression_toast_bytes bigint, before_compression_total_bytes bigint, after_compression_table_bytes bigint, after_compression_index_bytes bigint, after_compression_toast_bytes bigint, after_compression_total_bytes bigint, node_name name) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.compressed_chunk_remote_stats(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.compressed_chunk_remote_stats($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger() RETURNS TRIGGER
AS '@MODULE_PATHNAME@', 'ts_continuous_agg_invalidation_trigger' LANGUAGE C;

-- we have to prefix slices, schema_name and table_name parameter with _ here to not clash with output names otherwise plpgsql will complain
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk(hypertable regclass,_slices jsonb,_schema_name name=NULL,_table_name name=NULL,chunk_table regclass=NULL) RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB, created BOOLEAN) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.create_chunk(regclass,jsonb,name,name,regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.create_chunk($1,$2,$3,$4,$5);
END$$
SET search_path TO pg_catalog,pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_compressed_chunk(chunk regclass,chunk_table regclass,uncompressed_heap_size bigint,uncompressed_toast_size bigint,uncompressed_index_size bigint,compressed_heap_size bigint,compressed_toast_size bigint,compressed_index_size bigint,numrows_pre_compression bigint,numrows_post_compression bigint) RETURNS regclass LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.create_compressed_chunk(regclass,regclass,bigint,bigint,bigint,bigint,bigint,bigint,bigint,bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.create_compressed_chunk($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);
END$$
SET search_path TO pg_catalog,pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk(chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.drop_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.drop_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


-- We cannot create a wrapper function in plpgsql for the aggregate transition
-- functions because plpgsql cannot deal with datatype internal but since these
-- are used in an aggregation context and cannot be called directly and will
-- be used in conjunction with partialize_agg it is sufficient to have the
-- warning there.
CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_ffunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS anyelement
AS '@MODULE_PATHNAME@', 'ts_finalize_agg_ffunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_sfunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_finalize_agg_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE AGGREGATE _timescaledb_internal.finalize_agg(agg_name TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement) (
    SFUNC = _timescaledb_functions.finalize_agg_sfunc,
    STYPE = internal,
    FINALFUNC = _timescaledb_functions.finalize_agg_ffunc,
    FINALFUNC_EXTRA
);

CREATE OR REPLACE FUNCTION _timescaledb_internal.freeze_chunk(chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.freeze_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.freeze_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.generate_uuid() RETURNS uuid LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.generate_uuid() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.generate_uuid();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_approx_row_count(relation regclass) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_approx_row_count(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_approx_row_count($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(uncompressed_chunk regclass) RETURNS regclass LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_compressed_chunk_index_for_recompression(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_compressed_chunk_index_for_recompression($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_create_command(table_name name) RETURNS text LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_create_command(name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_create_command($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_git_commit() RETURNS TABLE(commit_tag TEXT, commit_hash TEXT, commit_time TIMESTAMPTZ) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_git_commit() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.get_git_commit();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_os_info() RETURNS TABLE(sysname TEXT, version TEXT, release TEXT, version_pretty TEXT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_os_info() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.get_os_info();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_partition_for_key(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_partition_for_key($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_hash(val anyelement) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_partition_hash(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_partition_hash($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_local_size(schema_name_in name,table_name_in name) RETURNS TABLE ( table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT, total_bytes BIGINT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.hypertable_local_size(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.hypertable_local_size($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.insert_blocker() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.insert_blocker();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(chunk_interval interval) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.interval_to_usec(interval) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.interval_to_usec($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.partialize_agg(arg anyelement) RETURNS bytea LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.partialize_agg(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.partialize_agg($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_compression_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_compression_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_compression_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_stat_history_retention(job_id integer,config jsonb) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_job_stat_history_retention(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.policy_job_stat_history_retention($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_stat_history_retention_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_job_stat_history_retention_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_job_stat_history_retention_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_refresh_continuous_aggregate_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_refresh_continuous_aggregate_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_refresh_continuous_aggregate_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_reorder_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_reorder_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_reorder_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_retention_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_retention_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_retention_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.process_ddl_event() RETURNS event_trigger LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.process_ddl_event() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.process_ddl_event();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.range_value_to_pretty(time_value bigint,column_type regtype) RETURNS text LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.range_value_to_pretty(bigint,regtype) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.range_value_to_pretty($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(uncompressed_chunk regclass,if_compressed boolean=false) RETURNS regclass LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.recompress_chunk_segmentwise(regclass,boolean) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.recompress_chunk_segmentwise($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.relation_size(relation regclass) RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.relation_size(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.relation_size($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.restart_background_workers() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.restart_background_workers();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.show_chunk(chunk regclass) RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.show_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.show_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.start_background_workers() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.start_background_workers() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.start_background_workers();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.stop_background_workers() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.stop_background_workers() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.stop_background_workers();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.subtract_integer_from_now(hypertable_relid regclass,lag bigint) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.subtract_integer_from_now(regclass,bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.subtract_integer_from_now($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val anyelement) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.time_to_internal(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.time_to_internal($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_date(unixtime_us bigint) RETURNS date LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_date(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_date($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_interval(unixtime_us bigint) RETURNS interval LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_interval(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_interval($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us bigint) RETURNS timestamp with time zone LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_timestamp(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_timestamp($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_without_timezone(unixtime_us bigint) RETURNS timestamp without time zone LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_timestamp_without_timezone(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_timestamp_without_timezone($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts timestamp with time zone) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_unix_microseconds(timestamp with time zone) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_unix_microseconds($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_loaded() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.tsl_loaded() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.tsl_loaded();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.unfreeze_chunk(chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.unfreeze_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.unfreeze_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_create_plan(_cagg_data _timescaledb_catalog.continuous_agg,_cagg_name_new text,_override boolean=false,_drop_old boolean=false) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_create_plan(_timescaledb_catalog.continuous_agg,text,boolean,boolean) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_create_plan($1,$2,$3,$4);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_data(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_copy_data(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_copy_data($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_policies(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_copy_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_copy_policies($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_create_new_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_create_new_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_create_new_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_disable_policies(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_disable_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_disable_policies($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_drop_old_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_drop_old_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_drop_old_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_enable_policies(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_enable_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_enable_policies($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_override_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_override_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_override_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_plan(_cagg_data _timescaledb_catalog.continuous_agg) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_plan(_timescaledb_catalog.continuous_agg) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_plan($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_compression(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_compression(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_compression($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_recompression(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_recompression(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_recompression($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_refresh_continuous_aggregate(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_refresh_continuous_aggregate(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_refresh_continuous_aggregate($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_reorder(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_reorder(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_reorder($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_retention(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_retention(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_retention($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;



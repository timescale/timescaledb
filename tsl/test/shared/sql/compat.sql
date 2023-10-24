-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

SELECT _timescaledb_internal.alter_job_set_hypertable_id(0,0);
SELECT _timescaledb_internal.attach_osm_table_chunk(0,0);
SELECT _timescaledb_internal.cagg_migrate_plan_exists(0);
SELECT _timescaledb_internal.cagg_migrate_pre_validation(NULL,NULL,NULL);
SELECT _timescaledb_internal.cagg_watermark(0);
SELECT _timescaledb_internal.cagg_watermark_materialized(0);
SELECT _timescaledb_internal.calculate_chunk_interval(0,0,0);
SELECT _timescaledb_internal.chunk_constraint_add_table_constraint(NULL);
SELECT _timescaledb_internal.chunk_drop_replica(0,NULL);
SELECT _timescaledb_internal.chunk_id_from_relid(0);
SELECT _timescaledb_internal.chunk_index_clone(0);
SELECT _timescaledb_internal.chunk_index_replace(0,0);
SELECT _timescaledb_internal.chunk_status(0);
SELECT _timescaledb_internal.chunks_in(NULL,NULL);
SELECT _timescaledb_internal.chunks_local_size(NULL,NULL);
SELECT _timescaledb_internal.chunks_remote_size(NULL,NULL);
SELECT _timescaledb_internal.compressed_chunk_local_stats(NULL,NULL);
SELECT _timescaledb_internal.compressed_chunk_remote_stats(NULL,NULL);
SELECT _timescaledb_internal.create_chunk(0,NULL,NULL,NULL,0);
SELECT _timescaledb_internal.create_chunk_replica_table(0,NULL);
SELECT _timescaledb_internal.create_chunk_table(0,NULL,NULL,NULL);
SELECT _timescaledb_internal.create_compressed_chunk(0,0,0,0,0,0,0,0,0,0);
SELECT _timescaledb_internal.data_node_chunk_info(NULL,NULL,NULL);
SELECT _timescaledb_internal.data_node_compressed_chunk_stats(NULL,NULL,NULL);
SELECT _timescaledb_internal.data_node_hypertable_info(NULL,NULL,NULL);
SELECT _timescaledb_internal.data_node_index_size(NULL,NULL,NULL);
SELECT _timescaledb_internal.drop_chunk(0);
SELECT _timescaledb_internal.drop_dist_ht_invalidation_trigger(0);
SELECT _timescaledb_internal.drop_stale_chunks(NULL,NULL);
SELECT _timescaledb_internal.freeze_chunk(0);
SELECT FROM _timescaledb_internal.generate_uuid();
SELECT _timescaledb_internal.get_approx_row_count(0);
SELECT _timescaledb_internal.get_chunk_colstats(0);
SELECT _timescaledb_internal.get_chunk_relstats(0);
SELECT _timescaledb_internal.get_compressed_chunk_index_for_recompression(0);
SELECT _timescaledb_internal.get_create_command(NULL);
SELECT pg_typeof(_timescaledb_internal.get_git_commit());
SELECT pg_typeof(_timescaledb_internal.get_os_info());
SELECT _timescaledb_internal.get_partition_for_key(NULL::text);
SELECT _timescaledb_internal.get_partition_hash(NULL::text);
SELECT pg_typeof(_timescaledb_internal.health());
SELECT _timescaledb_internal.hypertable_constraint_add_table_fk_constraint(NULL,NULL,NULL,0);
SELECT _timescaledb_internal.hypertable_invalidation_log_delete(0);
SELECT _timescaledb_internal.hypertable_local_size(NULL,NULL);
SELECT _timescaledb_internal.hypertable_remote_size(NULL,NULL);
SELECT _timescaledb_internal.indexes_local_size(NULL,NULL);
SELECT _timescaledb_internal.indexes_remote_size(NULL,NULL,NULL);
SELECT _timescaledb_internal.interval_to_usec(NULL);
SELECT _timescaledb_internal.invalidation_cagg_log_add_entry(0,0,0);
SELECT _timescaledb_internal.invalidation_hyper_log_add_entry(0,0,0);
SELECT _timescaledb_internal.invalidation_process_cagg_log(0,0,0,0,0,NULL,NULL,NULL);
SELECT _timescaledb_internal.invalidation_process_cagg_log(0,0,0,0,0,NULL,NULL,NULL,NULL);
SELECT _timescaledb_internal.invalidation_process_hypertable_log(0,0,0,NULL,NULL,NULL);
SELECT _timescaledb_internal.invalidation_process_hypertable_log(0,0,0,NULL,NULL,NULL,NULL);
SELECT _timescaledb_internal.materialization_invalidation_log_delete(0);
SELECT _timescaledb_internal.partialize_agg(NULL::text);
SELECT _timescaledb_internal.ping_data_node(NULL,NULL);
SELECT _timescaledb_internal.policy_compression_check(NULL);
SELECT _timescaledb_internal.policy_job_error_retention(0,NULL);
SELECT _timescaledb_internal.policy_job_error_retention_check(NULL);
SELECT _timescaledb_internal.policy_refresh_continuous_aggregate_check(NULL);
SELECT _timescaledb_internal.policy_reorder_check(NULL);
SELECT _timescaledb_internal.policy_retention_check(NULL);
SELECT _timescaledb_internal.range_value_to_pretty(0,0);
SELECT _timescaledb_internal.recompress_chunk_segmentwise(0,true);
SELECT _timescaledb_internal.relation_size(0);
SELECT _timescaledb_internal.remote_txn_heal_data_node(0);
SELECT _timescaledb_internal.restart_background_workers();
SELECT _timescaledb_internal.set_chunk_default_data_node(0,NULL);
SELECT _timescaledb_internal.set_dist_id(NULL);
SELECT _timescaledb_internal.set_peer_dist_id(NULL);
SELECT _timescaledb_internal.show_chunk(0);
SELECT pg_typeof(_timescaledb_internal.show_connection_cache());
SELECT _timescaledb_internal.start_background_workers();
SELECT _timescaledb_internal.stop_background_workers();
SELECT _timescaledb_internal.subtract_integer_from_now(0,0);
SELECT _timescaledb_internal.time_to_internal(NULL::timestamptz);
SELECT _timescaledb_internal.to_date(0);
SELECT _timescaledb_internal.to_interval(0);
SELECT _timescaledb_internal.to_timestamp(0);
SELECT _timescaledb_internal.to_timestamp_without_timezone(0);
SELECT _timescaledb_internal.to_unix_microseconds(NULL);
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.unfreeze_chunk(0);
SELECT _timescaledb_internal.validate_as_data_node();
CALL _timescaledb_internal.cagg_migrate_create_plan(NULL,NULL,true,true);
CALL _timescaledb_internal.cagg_migrate_execute_copy_data(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_copy_policies(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_create_new_cagg(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_disable_policies(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_drop_old_cagg(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_enable_policies(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_override_cagg(NULL,NULL);
CALL _timescaledb_internal.cagg_migrate_execute_plan(NULL);
CALL _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg(NULL,NULL);
CALL _timescaledb_internal.policy_compression(0,NULL);
CALL _timescaledb_internal.policy_compression_execute(0,0,NULL::interval,0,true,true,true);
CALL _timescaledb_internal.policy_recompression(0,NULL);
CALL _timescaledb_internal.policy_refresh_continuous_aggregate(0,NULL);
CALL _timescaledb_internal.policy_reorder(0,NULL);
CALL _timescaledb_internal.policy_retention(0,NULL);

CALL _timescaledb_internal.wait_subscription_sync(NULL,NULL,0,0);
\set ON_ERROR_STOP 1

-- tests for the cagg invalidation trigger on the deprecated schema
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION NULL
);

SELECT hypertable_id FROM create_hypertable('sensor_data','time') \gset

CREATE MATERIALIZED VIEW sensor_data_hourly WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT time_bucket(INTERVAL '1 hour', time),
       min(time),
       max(time)
FROM sensor_data
GROUP BY 1
WITH DATA;

DROP TRIGGER ts_cagg_invalidation_trigger ON sensor_data;

CREATE TRIGGER ts_cagg_invalidation_trigger
    AFTER INSERT OR DELETE OR UPDATE ON sensor_data
    FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger(:'hypertable_id');

INSERT INTO sensor_data values('1980-01-01 00:00:00-00', 1);
CALL refresh_continuous_aggregate('sensor_data_hourly', NULL, NULL);
-- should not return rows because there's old invalid regions
SELECT lowest_modified_value, greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :'hypertable_id';

-- insert old data to create invalidation log
INSERT INTO sensor_data values('1979-12-31 00:00:00-00', 1);
-- should return the invalidation log generated by previous insert
SELECT lowest_modified_value, greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :'hypertable_id';

DROP MATERIALIZED VIEW sensor_data_hourly;
DROP TABLE sensor_data CASCADE;

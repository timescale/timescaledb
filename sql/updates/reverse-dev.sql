DROP FUNCTION IF EXISTS _timescaledb_internal.block_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.allow_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.refresh_continuous_aggregate;
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_table;
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_replica_table;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_drop_replica;
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk;
DROP FUNCTION IF EXISTS _timescaledb_internal.copy_chunk_data;
DROP PROCEDURE IF EXISTS _timescaledb_internal.wait_subscription_sync;
DROP PROCEDURE IF EXISTS timescaledb_experimental.move_chunk;
DROP PROCEDURE IF EXISTS timescaledb_experimental.copy_chunk;
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_copy_activity;
DROP SEQUENCE IF EXISTS _timescaledb_catalog.chunk_copy_activity_id_seq;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;
DROP SCHEMA IF EXISTS timescaledb_experimental CASCADE;

-- We need to rewrite all continuous aggregates to make sure that the
-- queries do not contain qualification. They will be re-written in
-- the post-update script as well, but the previous version does not
-- process all continuous aggregates, leaving some with qualification
-- for the standard functions. To make this work, we need to
-- temporarily set the update stage to the post-update stage, which
-- will allow the ALTER MATERIALIZED VIEW to rewrite the query. If
-- that is not done, the TimescaleDB-specific hooks will not be used
-- and you will get an error message saying that, for example,
-- `conditions_summary` is not a materialized view.
SET timescaledb.update_script_stage TO 'post';
DO $$
DECLARE
 vname regclass;
 materialized_only bool;
 altercmd text;
 ts_version TEXT;
BEGIN
    FOR vname, materialized_only IN select format('%I.%I', cagg.user_view_schema, cagg.user_view_name)::regclass, cagg.materialized_only from _timescaledb_catalog.continuous_agg cagg
    LOOP
	altercmd := format('ALTER MATERIALIZED VIEW %s SET (timescaledb.materialized_only=%L) ', vname::text, materialized_only);
        EXECUTE altercmd;
    END LOOP;
    EXCEPTION WHEN OTHERS THEN RAISE;
END
$$;
RESET timescaledb.update_script_stage;

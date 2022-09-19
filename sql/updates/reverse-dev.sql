
-- gapfill with timezone support
DROP FUNCTION @extschema@.time_bucket_gapfill(INTERVAL,TIMESTAMPTZ,TEXT,TIMESTAMPTZ,TIMESTAMPTZ);

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_pkey PRIMARY KEY(chunk_id,compressed_chunk_id);

DROP PROCEDURE @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);

CREATE PROCEDURE @extschema@.cagg_migrate (
    _cagg REGCLASS,
    _override BOOLEAN DEFAULT FALSE,
    _drop_old BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg_schema TEXT;
    _cagg_name TEXT;
    _cagg_name_new TEXT;
    _cagg_data _timescaledb_catalog.continuous_agg;
BEGIN
    SELECT nspname, relname
    INTO _cagg_schema, _cagg_name
    FROM pg_catalog.pg_class
    JOIN pg_catalog.pg_namespace ON pg_namespace.oid OPERATOR(pg_catalog.=) pg_class.relnamespace
    WHERE pg_class.oid OPERATOR(pg_catalog.=) _cagg::pg_catalog.oid;

    -- maximum size of an identifier in Postgres is 63 characters, se we need to left space for '_new'
    _cagg_name_new := pg_catalog.format('%s_new', pg_catalog.substr(_cagg_name, 1, 59));

    -- pre-validate the migration and get some variables
    _cagg_data := _timescaledb_internal.cagg_migrate_pre_validation(_cagg_schema, _cagg_name, _cagg_name_new);

    -- create new migration plan
    CALL _timescaledb_internal.cagg_migrate_create_plan(_cagg_data, _cagg_name_new, _override, _drop_old);
    COMMIT;

    -- execute the migration plan
    CALL _timescaledb_internal.cagg_migrate_execute_plan(_cagg_data);

    -- finish the migration plan
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan
    SET end_ts = pg_catalog.clock_timestamp()
    WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _cagg_data.mat_hypertable_id;
END;
$BODY$;

DROP FUNCTION _timescaledb_internal.policy_job_error_retention(integer, JSONB);
DROP FUNCTION _timescaledb_internal.policy_job_error_retention_check(JSONB);
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;

ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.job_errors;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.job_errors;

DROP VIEW timescaledb_information.job_errors;
DROP TABLE _timescaledb_internal.job_errors;

-- drop dependent views
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;

ALTER TABLE _timescaledb_internal.bgw_job_stat
DROP COLUMN flags;
-- need to recreate the bgw_job_stats table because dropping the column 
-- will not remove it from the pg_attribute table

CREATE TABLE _timescaledb_internal.bgw_job_stat_tmp (
    LIKE _timescaledb_internal.bgw_job_stat
    INCLUDING ALL
    -- indexes and constraintes will be created later to keep original names
    EXCLUDING INDEXES
    EXCLUDING CONSTRAINTS
);

INSERT INTO _timescaledb_internal.bgw_job_stat_tmp
SELECT
    job_id,
    last_start,
    last_finish,
    next_start,
    last_successful_finish,
    last_run_success,
    total_runs,
    total_duration,
    total_successes,
    total_failures,
    total_crashes,
    consecutive_failures,
    consecutive_crashes
FROM
    _timescaledb_internal.bgw_job_stat;

DROP TABLE _timescaledb_internal.bgw_job_stat;

ALTER TABLE _timescaledb_internal.bgw_job_stat_tmp
    RENAME TO bgw_job_stat;
ALTER TABLE _timescaledb_internal.bgw_job_stat
    ADD CONSTRAINT bgw_job_stat_pkey PRIMARY KEY (job_id),
    ADD CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY (job_id) 
    REFERENCES _timescaledb_config.bgw_job (id) ON DELETE CASCADE;

GRANT SELECT ON TABLE _timescaledb_internal.bgw_job_stat TO PUBLIC;

-- gapfill with timezone support
CREATE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamptz_timezone_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_pkey PRIMARY KEY(chunk_id);

CREATE TABLE _timescaledb_internal.job_errors (
  job_id integer not null, 
  pid integer,
  start_time timestamptz,
  finish_time timestamptz,
  error_data jsonb
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_internal.job_errors', '');

CREATE VIEW timescaledb_information.job_errors AS 
SELECT
    job_id,
    error_data ->> 'proc_schema' as proc_schema,
    error_data ->> 'proc_name' as proc_name,
    pid,
    start_time,
    finish_time,
    error_data ->> 'sqlerrcode' AS sqlerrcode,
    CASE WHEN error_data ->>'message' IS NOT NULL THEN 
      CASE WHEN error_data ->>'detail' IS NOT NULL THEN 
        CASE WHEN error_data ->>'hint' IS NOT NULL THEN concat(error_data ->>'message', '. ', error_data ->>'detail', '. ', error_data->>'hint')
        ELSE concat(error_data ->>'message', ' ', error_data ->>'detail')
        END
      ELSE
        CASE WHEN error_data ->>'hint' IS NOT NULL THEN concat(error_data ->>'message', '. ', error_data->>'hint')
        ELSE error_data ->>'message'
        END
      END
    ELSE
      'job crash detected, see server logs'
    END
    AS err_message
FROM
    _timescaledb_internal.job_errors;

ALTER TABLE _timescaledb_internal.bgw_job_stat ADD COLUMN flags integer;
UPDATE _timescaledb_internal.bgw_job_stat SET flags = 0;

ALTER TABLE _timescaledb_internal.bgw_job_stat 
ALTER COLUMN flags SET NOT NULL,
ALTER COLUMN flags SET DEFAULT 0;


DROP FUNCTION IF EXISTS @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.add_continuous_aggregate_policy(REGCLASS, "any", "any", INTERVAL, BOOL);
DROP FUNCTION IF EXISTS @extschema@.add_job(REGPROC, INTERVAL, JSONB, TIMESTAMPTZ, BOOL, REGPROC);

DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_experimental.policies;

-- rebuild _timescaledb_config.bgw_job
CREATE TABLE _timescaledb_config.bgw_job_tmp AS SELECT * FROM _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;
ALTER TABLE _timescaledb_internal.bgw_job_stat DROP CONSTRAINT IF EXISTS bgw_job_stat_job_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT IF EXISTS bgw_policy_chunk_stats_job_id_fkey;

CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;

DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_bgw_job_seq_value;
DROP TABLE _timescaledb_internal.tmp_bgw_job_seq_value;

-- new table as we want it
CREATE TABLE _timescaledb_config.bgw_job (
  id INTEGER PRIMARY KEY DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
  application_name name NOT NULL,
  schedule_interval interval NOT NULL,
  max_runtime interval NOT NULL,
  max_retries integer NOT NULL,
  retry_period interval NOT NULL,
  proc_schema name NOT NULL,
  proc_name name NOT NULL,
  owner name NOT NULL DEFAULT CURRENT_ROLE,
  scheduled bool NOT NULL DEFAULT TRUE,
  fixed_schedule bool not null default true,
  initial_start timestamptz,
  hypertable_id  integer REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  config jsonb ,
  check_schema NAME,
  check_name NAME 
);

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;
CREATE INDEX bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job(proc_schema,proc_name,hypertable_id);

INSERT INTO _timescaledb_config.bgw_job(
    id, application_name, schedule_interval, 
    max_runtime, max_retries, retry_period, 
    proc_schema, proc_name, owner, scheduled, 
    hypertable_id, config, check_schema, check_name, 
    fixed_schedule
)
SELECT id, application_name, schedule_interval, max_runtime, max_retries, retry_period,
    proc_schema, proc_name, owner, scheduled, hypertable_id, config, check_schema, check_name, FALSE
FROM _timescaledb_config.bgw_job_tmp ORDER BY id;

DROP TABLE _timescaledb_config.bgw_job_tmp;
ALTER TABLE _timescaledb_internal.bgw_job_stat ADD CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT bgw_policy_chunk_stats_job_id_fkey FOREIGN KEY(job_id) REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;

-- do simple CREATE for the functions with modified signatures
CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
continuous_aggregate REGCLASS, start_offset "any", 
end_offset "any", schedule_interval INTERVAL, 
if_not_exists BOOL = false, 
initial_start TIMESTAMPTZ = NULL)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_add'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_compression_policy(
        hypertable REGCLASS, compress_after "any", 
        if_not_exists BOOL = false,
        schedule_interval INTERVAL = NULL, 
        initial_start TIMESTAMPTZ = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_retention_policy(
       relation REGCLASS,
       drop_after "any",
       if_not_exists BOOL = false,
       schedule_interval INTERVAL = NULL,
       initial_start TIMESTAMPTZ = NULL
)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true,
  check_config REGPROC DEFAULT NULL,
  fixed_schedule BOOL DEFAULT TRUE
) RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_job_add' LANGUAGE C VOLATILE;

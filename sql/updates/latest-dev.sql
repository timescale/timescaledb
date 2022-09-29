
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


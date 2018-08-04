-- Trigger that blocks INSERTs on the hypertable's root table
CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker' LANGUAGE C;

-- Drop all pre-0.11.1 insert_blockers from hypertables and add the new, visible trigger
CREATE FUNCTION _timescaledb_internal.insert_blocker_trigger_add(relid REGCLASS) RETURNS OID
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker_trigger_add' LANGUAGE C VOLATILE STRICT;

SELECT _timescaledb_internal.insert_blocker_trigger_add(h.relid)
FROM (SELECT format('%I.%I', schema_name, table_name)::regclass AS relid FROM _timescaledb_catalog.hypertable) AS h;

DROP FUNCTION _timescaledb_internal.insert_blocker_trigger_add(REGCLASS);

CREATE SCHEMA IF NOT EXISTS _timescaledb_config;
GRANT USAGE ON SCHEMA _timescaledb_config TO PUBLIC;

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_job (
    id                  SERIAL      PRIMARY KEY,
    application_name    NAME        NOT NULL,
    job_type            NAME        NOT NULL,
    schedule_interval   INTERVAL    NOT NULL,
    max_runtime         INTERVAL    NOT NULL,
    max_retries         INT         NOT NULL,
    retry_period        INTERVAL    NOT NULL,
    CONSTRAINT  valid_job_type CHECK (job_type IN ('update_check'))
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_config.bgw_job','id'), '');

CREATE TABLE IF NOT EXISTS _timescaledb_internal.bgw_job_stat (
    job_id                  INT         PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    last_start              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_finish             TIMESTAMPTZ NOT NULL,
    next_start              TIMESTAMPTZ NOT NULL,
    last_run_success        BOOL        NOT NULL,
    total_runs              BIGINT      NOT NULL,
    total_duration          INTERVAL    NOT NULL,
    total_successes         BIGINT      NOT NULL,
    total_failures          BIGINT      NOT NULL,
    total_crashes           BIGINT      NOT NULL,
    consecutive_failures    INT         NOT NULL,
    consecutive_crashes     INT         NOT NULL
);
--The job_stat table is not dumped by pg_dump on purpose because
--the statistics probably aren't very meaningful across instances.

GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_internal.bgw_job_stat TO PUBLIC;

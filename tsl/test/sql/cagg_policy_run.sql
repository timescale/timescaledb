-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- The tests in this file use mock timestamps to test policies
-- for timestamp based tables and can be run only with debug builds.

SET ROLE :ROLE_DEFAULT_PERM_USER;


--- code coverage tests : add policy for timestamp and date based table ---
CREATE TABLE continuous_agg_max_mat_date(time DATE);
SELECT create_hypertable('continuous_agg_max_mat_date', 'time');
CREATE MATERIALIZED VIEW max_mat_view_date
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('1 days', time), count(*)
        FROM continuous_agg_max_mat_date
        GROUP BY 1 WITH NO DATA;

SELECT add_continuous_aggregate_policy('max_mat_view_date', '3 days', '1 day', '1 day'::interval) as job_id \gset
SELECT config FROM _timescaledb_catalog.bgw_job
WHERE id = :job_id;

INSERT INTO continuous_agg_max_mat_date
    SELECT generate_series('2019-09-01'::date, '2019-09-10'::date, '1 day');
--- to prevent NOTICES set message level to warning
SET client_min_messages TO warning;
SET timescaledb.current_timestamp_mock = '2019-09-10 00:00';
CALL run_job(:job_id);
SELECT * FROM max_mat_view_date order by 1;
RESET client_min_messages ;
DROP MATERIALIZED VIEW max_mat_view_date;

CREATE TABLE continuous_agg_timestamp(time TIMESTAMP);
SELECT create_hypertable('continuous_agg_timestamp', 'time');

CREATE MATERIALIZED VIEW max_mat_view_timestamp
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('7 days', time), count(*)
        FROM continuous_agg_timestamp
        GROUP BY 1 WITH NO DATA;

SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '15 days', '1 h'::interval , '1 h'::interval) as job_id \gset
INSERT INTO continuous_agg_timestamp
    SELECT generate_series('2019-09-01 00:00'::timestamp, '2019-09-10 00:00'::timestamp, '1 day');
--- to prevent NOTICES set message level to warning
SET client_min_messages TO warning;
SET timescaledb.current_timestamp_mock = '2019-09-11 00:00';
CALL run_job(:job_id);
SELECT * FROM max_mat_view_timestamp;
RESET client_min_messages ;

--- Test compress_after_refresh config option ---
CREATE TABLE conditions(time TIMESTAMPTZ NOT NULL, device INT, temp FLOAT);
SELECT create_hypertable('conditions', 'time', chunk_time_interval => INTERVAL '1 day');
INSERT INTO conditions
SELECT t, d, 1.0
FROM generate_series('2025-01-01'::timestamptz, '2025-01-05'::timestamptz, INTERVAL '1 hour') t,
     generate_series(1, 3) d;

CREATE MATERIALIZED VIEW conditions_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 day', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1, 2 WITH NO DATA;

ALTER MATERIALIZED VIEW conditions_daily SET (timescaledb.compress = true);

CREATE VIEW cagg_chunks AS
SELECT chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = (
    SELECT materialization_hypertable_name FROM timescaledb_information.continuous_aggregates
    WHERE view_name = 'conditions_daily')
ORDER BY chunk_name;

-- Default compress_after_refresh = true: chunks should be compressed after refresh
SELECT add_continuous_aggregate_policy('conditions_daily',
    start_offset => NULL, end_offset => NULL,
    schedule_interval => INTERVAL '1 day') AS job_id \gset

-- Key is absent until explicitly set; default value lives in C code
SELECT config ? 'compress_after_refresh' AS has_key
FROM _timescaledb_catalog.bgw_job WHERE id = :job_id;

CALL run_job(:job_id);
SELECT * FROM cagg_chunks;

-- compress_after_refresh = false: chunks must remain uncompressed across refresh
SELECT config FROM alter_job(:job_id,
    config => jsonb_set(
        (SELECT config FROM _timescaledb_catalog.bgw_job WHERE id = :job_id),
        '{compress_after_refresh}', 'false'));

SELECT decompress_chunk(ch) FROM show_chunks('conditions_daily') ch;
INSERT INTO conditions
SELECT t, 4, 2.0 FROM generate_series('2025-01-01'::timestamptz, '2025-01-05'::timestamptz, INTERVAL '1 hour') t;
CALL run_job(:job_id);
SELECT * FROM cagg_chunks;

-- Flip back to true: next run compresses again
SELECT config FROM alter_job(:job_id,
    config => jsonb_set(
        (SELECT config FROM _timescaledb_catalog.bgw_job WHERE id = :job_id),
        '{compress_after_refresh}', 'true'));

INSERT INTO conditions
SELECT t, 5, 3.0 FROM generate_series('2025-01-01'::timestamptz, '2025-01-05'::timestamptz, INTERVAL '1 hour') t;
CALL run_job(:job_id);
SELECT * FROM cagg_chunks;

SELECT delete_job(:job_id);
DROP VIEW cagg_chunks;
DROP MATERIALIZED VIEW conditions_daily;
DROP TABLE conditions;


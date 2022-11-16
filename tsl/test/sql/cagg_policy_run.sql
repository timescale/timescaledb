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
SELECT config FROM _timescaledb_config.bgw_job
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


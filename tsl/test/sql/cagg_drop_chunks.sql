-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

SET timezone TO PST8PDT;

--
-- Check that drop chunks with a unique constraint works as expected.
--
CREATE TABLE clients (
       id SERIAL PRIMARY KEY,
       name TEXT NOT NULL,
       UNIQUE(name)
);

CREATE TABLE records (
    time TIMESTAMPTZ NOT NULL,
    clientId INT NOT NULL REFERENCES clients(id),
    value DOUBLE PRECISION,
    UNIQUE(time, clientId)
);

SELECT * FROM create_hypertable('records', 'time',
       chunk_time_interval => INTERVAL '1h');

CREATE MATERIALIZED VIEW records_monthly
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS
        SELECT time_bucket('1d', time) as bucket,
            clientId,
            avg(value) as value_avg,
            max(value)-min(value) as value_spread
        FROM records GROUP BY bucket, clientId WITH NO DATA;

INSERT INTO clients(name) VALUES ('test-client');

INSERT INTO records
SELECT generate_series('2000-03-01'::timestamptz,'2000-04-01','1 day'),1,3.14;

SELECT * FROM records_monthly;

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'records_monthly' ORDER BY range_start;

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'records' ORDER BY range_start;

CALL refresh_continuous_aggregate('records_monthly', NULL, NULL);

\set VERBOSITY default
SELECT drop_chunks('records', '2000-03-16'::timestamptz);

\set VERBOSITY terse
DROP MATERIALIZED VIEW records_monthly;
DROP TABLE records;
DROP TABLE clients;
\set VERBOSITY default

CREATE PROCEDURE refresh_cagg_by_chunk_range(_cagg REGCLASS, _hypertable REGCLASS, _older_than INTEGER)
AS
$$
DECLARE
    _r RECORD;
BEGIN
    WITH _chunks AS (
        SELECT relname, nspname
        FROM show_chunks(_hypertable, _older_than) AS relid
        JOIN pg_catalog.pg_class ON pg_class.oid = relid AND pg_class.relkind = 'r'
        JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    )
    SELECT MIN(range_start) AS range_start, MAX(range_end) AS range_end
    INTO _r
    FROM
        _chunks
        JOIN _timescaledb_catalog.chunk ON chunk.schema_name = _chunks.nspname AND chunk.table_name = _chunks.relname
        JOIN _timescaledb_catalog.chunk_constraint ON chunk_id = chunk.id
        JOIN _timescaledb_catalog.dimension_slice ON dimension_slice.id = dimension_slice_id;

    RAISE INFO 'range_start=% range_end=%', _r.range_start::int, _r.range_end::int;
    CALL refresh_continuous_aggregate(_cagg, _r.range_start::int, _r.range_end::int + 1);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as
    $$ SELECT 125 $$;

CREATE TABLE conditions(time_int INT NOT NULL, value FLOAT);
SELECT create_hypertable('conditions', 'time_int', chunk_time_interval => 4);

INSERT INTO conditions
SELECT time_val, 1 FROM generate_series(0, 19, 1) AS time_val;

SELECT set_integer_now_func('conditions', 'test_int_now');

CREATE MATERIALIZED VIEW conditions_2
    WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
    AS
        SELECT time_bucket(2, time_int) as bucket,
            SUM(value), COUNT(value)
        FROM conditions GROUP BY bucket WITH DATA;

SELECT * FROM conditions_2 ORDER BY bucket;

UPDATE conditions SET value = 4.00 WHERE time_int = 0;
UPDATE conditions SET value = 4.00 WHERE time_int = 6;

CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', 4);
SELECT drop_chunks('conditions', 4);

SELECT * FROM conditions_2 ORDER BY bucket;

CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', 8);
SELECT * FROM conditions_2 ORDER BY bucket;

UPDATE conditions SET value = 4.00 WHERE time_int = 19;

SELECT drop_chunks('conditions', 8);
CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', 12);
SELECT * FROM conditions_2 ORDER BY bucket;

CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', NULL);
SELECT * FROM conditions_2 ORDER BY bucket;

DROP PROCEDURE refresh_cagg_by_chunk_range(REGCLASS, REGCLASS, INTEGER);

--
-- Test drop_chunks with continuous aggregates and watermark protection
--
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time',
    chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO sensor_data
SELECT
    timestamp '2024-01-01' + (i * INTERVAL '4 hours') AS time,
    (i % 5) + 1 AS sensor_id,
    15 + 15 * random() AS temperature,
    30 + 60 * random() AS humidity,
    980 + 40 * random() AS pressure
FROM generate_series(0, 25) AS i;

CREATE MATERIALIZED VIEW sensor_hourly_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    sensor_id,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    COUNT(*) AS reading_count
FROM sensor_data
GROUP BY bucket, sensor_id
WITH NO DATA;

-- Checks range start and end for chunks
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;

-- Completely refresh the aggregate
CALL refresh_continuous_aggregate('sensor_hourly_avg', NULL, NULL);

SELECT count(*) AS ht_before FROM show_chunks('sensor_data');

-- Insert more data after the CAgg watermark
INSERT INTO sensor_data
SELECT
    timestamp '2024-02-01' + (i * INTERVAL '4 hours') AS time,
    (i % 5) + 1 AS sensor_id,
    15 + 15 * random() AS temperature,
    30 + 60 * random() AS humidity,
    980 + 40 * random() AS pressure
FROM generate_series(0, 25) AS i;

-- View ranges before drop chunks
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;

-- Verify watermark
SELECT h.id AS mat_hypertable_id
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
  WHERE ca.user_view_name = 'sensor_hourly_avg' \gset

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id));

\set VERBOSITY terse
BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', newer_than => '2023-12-31 17:00:00-07'::timestamp with time zone);

SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;

ROLLBACK;

-- Chunk with unrefreshed data can be dropped directly using DROP TABLE ...
BEGIN;
SELECT format('%I.%I', chunk_schema, chunk_name) AS chunk_to_drop
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start LIMIT 1 \gset

SELECT :'chunk_to_drop' AS dropped_chunk;

DROP TABLE :chunk_to_drop;

SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', older_than => '2024-02-05 17:00:00-07'::timestamp with time zone);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', newer_than => '2024-01-02 17:00:00-07'::timestamp with time zone, older_than => '2024-02-02 17:00:00-07'::timestamp with time zone);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;

-- Test force option
BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', newer_than => '2023-01-05 17:00:00-07'::timestamp with time zone, force => true);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;

\set VERBOSITY default

--
-- Test with multiple continuous aggregates to verify earliest watermark is used
--
-- Create a second continuous aggregate on the same hypertable (NOT hierarchical cagg)
CREATE MATERIALIZED VIEW sensor_daily_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    sensor_id,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    COUNT(*) AS reading_count
FROM sensor_data
GROUP BY bucket, sensor_id
WITH NO DATA;

-- Refresh the second aggregate partially to a different point
CALL refresh_continuous_aggregate('sensor_daily_avg', '2024-01-01', '2024-01-03');

SELECT h.id AS mat_hypertable_id_daily
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
  WHERE ca.user_view_name = 'sensor_daily_avg' \gset

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id)) AS hourly_watermark;
SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id_daily)) AS daily_watermark;

\set VERBOSITY terse
BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', older_than => '2024-02-05 17:00:00-07'::timestamp with time zone);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', newer_than => '2024-01-01 17:00:00-07'::timestamp with time zone, older_than => '2024-02-02 17:00:00-07'::timestamp with time zone);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;
\set VERBOSITY default

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', newer_than => '2024-01-05 17:00:00-07'::timestamp with time zone, force => true);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;
\set VERBOSITY default

DROP MATERIALIZED VIEW sensor_daily_avg;

--
-- Test with hierarchical continuous aggregate
--
-- Create hierarchical cagg
CREATE MATERIALIZED VIEW sensor_daily_avg_hier
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    sensor_id,
    AVG(avg_temperature) AS avg_temperature,
    AVG(avg_humidity) AS avg_humidity,
    AVG(avg_pressure) AS avg_pressure,
    SUM(reading_count) AS reading_count
FROM sensor_hourly_avg
GROUP BY time_bucket('1 day', bucket), sensor_id
WITH NO DATA;

-- Refresh the hierarchical aggregate partially
CALL refresh_continuous_aggregate('sensor_daily_avg_hier', NULL, '2024-01-03');

-- Verify watermarks
SELECT h.id AS mat_hypertable_id_daily_hier
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
  WHERE ca.user_view_name = 'sensor_daily_avg_hier' \gset

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id)) AS hourly_watermark;
SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id_daily_hier)) AS daily_hierarchical_watermark;

\set VERBOSITY terse
BEGIN;

-- With hierarchical cagg, drop_chunks should still use the earliest watermark from the raw hypertable's caggs
-- The hierarchical cagg watermark should not affect raw hypertable chunk retention
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', older_than => '2024-02-05 17:00:00-07'::timestamp with time zone);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data', newer_than => '2024-01-01 17:00:00-07'::timestamp with time zone, older_than => '2024-02-02 17:00:00-07'::timestamp with time zone);
SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data'
  ORDER BY range_start;
ROLLBACK;
\set VERBOSITY default

-- Try drop_chunks on base cagg in the case of hierarchical caggs
-- Here, the watermark of the hierarchical CAgg matters but the base CAgg watermark shouldn't affect it

CALL refresh_continuous_aggregate('sensor_hourly_avg', NULL, NULL);

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id)) AS hourly_watermark;
SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id_daily_hier)) AS daily_hierarchical_watermark;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_hourly_avg', older_than => '2024-01-10 17:00:00-07'::timestamp with time zone);
 SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = (
      SELECT h.table_name
      FROM _timescaledb_catalog.continuous_agg ca
      JOIN _timescaledb_catalog.hypertable h ON
  h.id = ca.mat_hypertable_id
      WHERE ca.user_view_name = 'sensor_hourly_avg'
  )
  ORDER BY range_start;
ROLLBACK;

-- Test force option
BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_hourly_avg', older_than => '2024-01-10 17:00:00-07'::timestamp with time zone, force => true);
 SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = (
      SELECT h.table_name
      FROM _timescaledb_catalog.continuous_agg ca
      JOIN _timescaledb_catalog.hypertable h ON
  h.id = ca.mat_hypertable_id
      WHERE ca.user_view_name = 'sensor_hourly_avg'
  )
  ORDER BY range_start;
ROLLBACK;

-- Refresh hierarchical cagg completely
CALL refresh_continuous_aggregate('sensor_daily_avg_hier', NULL, NULL);

SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_hypertable_id_daily_hier)) AS daily_hierarchical_watermark;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_hourly_avg', older_than => '2024-01-10 17:00:00-07'::timestamp with time zone);
 SELECT chunk_name, range_start, range_end
  FROM timescaledb_information.chunks
  WHERE hypertable_name = (
      SELECT h.table_name
      FROM _timescaledb_catalog.continuous_agg ca
      JOIN _timescaledb_catalog.hypertable h ON
  h.id = ca.mat_hypertable_id
      WHERE ca.user_view_name = 'sensor_hourly_avg'
  )
  ORDER BY range_start;
ROLLBACK;

DROP MATERIALIZED VIEW sensor_daily_avg_hier;
DROP MATERIALIZED VIEW sensor_hourly_avg;
DROP TABLE sensor_data;

--
-- Test drop_chunks with integer time continuous aggregates
--
CREATE OR REPLACE FUNCTION integer_now_sensor_data() returns INT LANGUAGE SQL STABLE as
    $$ SELECT 150 $$;

CREATE TABLE sensor_data_int (
    time_int INT NOT NULL,
    sensor_id INTEGER NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data_int', 'time_int',
    chunk_time_interval => 10
);

SELECT set_integer_now_func('sensor_data_int', 'integer_now_sensor_data');

INSERT INTO sensor_data_int
SELECT
    i AS time_int,
    (i % 5) + 1 AS sensor_id,
    15 + 15 * random() AS temperature,
    30 + 60 * random() AS humidity,
    980 + 40 * random() AS pressure
FROM generate_series(0, 99, 4) AS i;

CREATE MATERIALIZED VIEW sensor_hourly_avg_int
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(5, time_int) AS bucket,
    sensor_id,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    COUNT(*) AS reading_count
FROM sensor_data_int
GROUP BY bucket, sensor_id
WITH NO DATA;

-- Check range start and end for chunks
SELECT chunk_name, range_start_integer, range_end_integer
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data_int'
  ORDER BY range_start_integer;

-- Refresh the aggregate completely
CALL refresh_continuous_aggregate('sensor_hourly_avg_int', NULL, NULL);

SELECT count(*) AS ht_before FROM show_chunks('sensor_data_int');

-- Insert more data after the CAgg watermark
INSERT INTO sensor_data_int
SELECT
    i AS time_int,
    (i % 5) + 1 AS sensor_id,
    15 + 15 * random() AS temperature,
    30 + 60 * random() AS humidity,
    980 + 40 * random() AS pressure
FROM generate_series(100, 199, 4) AS i;

-- View ranges before drop chunks
SELECT chunk_name, range_start_integer, range_end_integer
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data_int'
  ORDER BY range_start_integer;

-- Verify watermark
SELECT h.id AS mat_hypertable_id_int
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
  WHERE ca.user_view_name = 'sensor_hourly_avg_int' \gset

SELECT _timescaledb_functions.cagg_watermark(:mat_hypertable_id_int);

\set VERBOSITY terse
BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data_int', newer_than => 0);

SELECT chunk_name, range_start_integer, range_end_integer
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data_int'
  ORDER BY range_start_integer;

ROLLBACK;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data_int', older_than => 120);
SELECT chunk_name, range_start_integer, range_end_integer
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data_int'
  ORDER BY range_start_integer;
ROLLBACK;

BEGIN;
SELECT count(*) AS dropped_count FROM drop_chunks('sensor_data_int', newer_than => 10, older_than => 110);
SELECT chunk_name, range_start_integer, range_end_integer
  FROM timescaledb_information.chunks
  WHERE hypertable_name = 'sensor_data_int'
  ORDER BY range_start_integer;
ROLLBACK;
\set VERBOSITY default

DROP MATERIALIZED VIEW sensor_hourly_avg_int;
DROP TABLE sensor_data_int;

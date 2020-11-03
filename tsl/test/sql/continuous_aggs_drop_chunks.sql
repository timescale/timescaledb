-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

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
    WITH (timescaledb.continuous)
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

-- Test how caggs are afected by drop_chunk due to refresh on drop
-- Demonstrates issue #2592

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as 
    $$ SELECT 125 $$;

CREATE TABLE conditions(time_int INT NOT NULL, device INT, value FLOAT);
SELECT create_hypertable('conditions', 'time_int', chunk_time_interval => 10);

INSERT INTO conditions
SELECT time_val, time_val % 4, 3.14 FROM generate_series(0,100,1) AS time_val;

SELECT set_integer_now_func('conditions', 'test_int_now');
CREATE MATERIALIZED VIEW conditions_7
    WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
    AS
        SELECT time_bucket(7, time_int) as bucket,
            SUM(value), COUNT(value)
        FROM conditions GROUP BY bucket WITH DATA;
SELECT materialization_hypertable_schema||'.'||materialization_hypertable_name AS mat_hyper
FROM timescaledb_information.continuous_aggregates
WHERE view_name = 'conditions_7' \gset

CREATE VIEW see_cagg AS SELECT * FROM conditions_7 WHERE bucket < 70 ORDER BY bucket;
--CREATE VIEW see_mat AS SELECT bucket, count(*) FROM :mat_hyper WHERE bucket < 70 
--GROUP BY bucket ORDER BY bucket;

SELECT * FROM see_cagg;

-- This is the simplest case, when the updated bucket is inside chunk, so it is expected
-- always reflected.
UPDATE conditions SET value = 4.00 WHERE time_int = 2;

SELECT drop_chunks('conditions', 20);
SELECT * FROM see_cagg;

-- This case is update in a bucket, which crosses two chunks, which exist and both is going
-- to be dropped.
UPDATE conditions SET value = 4.00 WHERE time_int = 31;

-- Now outliers will be present after this second drop_chunks
SELECT drop_chunks('conditions', 40);
SELECT * FROM see_cagg;

-- This update is in the bucket, which crosses two chunks, where one chunk was already dropped
-- and empty, while the other will be dropped.
UPDATE conditions SET value = 4.00 WHERE time_int = 41;

SELECT drop_chunks('conditions', 60);
SELECT * FROM see_cagg;

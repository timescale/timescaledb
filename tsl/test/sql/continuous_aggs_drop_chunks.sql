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

CREATE VIEW records_monthly 
    WITH (timescaledb.continuous)
    AS 
        SELECT time_bucket('1d', time) as bucket, 
            clientId, 
            avg(value) as value_avg,
            max(value)-min(value) as value_spread 
        FROM records GROUP BY bucket, clientId;

INSERT INTO clients(name) VALUES ('test-client');

INSERT INTO records
SELECT generate_series('2000-03-01'::timestamptz,'2000-04-01','1 day'),1,3.14;

SET timescaledb.current_timestamp_mock = '2000-04-01';

SELECT * FROM records_monthly;

ALTER VIEW records_monthly SET (
   timescaledb.ignore_invalidation_older_than = '15 days'
);

SELECT chunk_table, ranges FROM chunk_relation_size('records_monthly');
SELECT chunk_table, ranges FROM chunk_relation_size('records');

REFRESH MATERIALIZED VIEW records_monthly;
REFRESH MATERIALIZED VIEW records_monthly;

\set VERBOSITY default
SELECT drop_chunks('2000-03-16'::timestamptz, 'records',
       cascade_to_materializations => FALSE);

------------------------------------------
-- Test behavior of drop on refresh in 1.7


CREATE TABLE conditions(time_int INT NOT NULL, device INT, value FLOAT);
SELECT create_hypertable('conditions', 'time_int', chunk_time_interval => 10);

INSERT INTO conditions 
    SELECT time_val, time_val % 4, 3.14 FROM generate_series(0,100,1) AS time_val;

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as 
    $$ SELECT 100 $$;
SELECT set_integer_now_func('conditions', 'test_int_now');

CREATE VIEW conditions_7
    WITH (timescaledb.continuous, timescaledb.materialized_only = true)
    AS
        SELECT time_bucket(7, time_int) as bucket,
            SUM(value), COUNT(value)
        FROM conditions GROUP BY bucket;
REFRESH MATERIALIZED VIEW conditions_7;

ALTER VIEW conditions_7 SET (timescaledb.ignore_invalidation_older_than = 100);

CREATE VIEW see_cagg AS SELECT * FROM conditions_7 WHERE bucket < 70 ORDER BY bucket;

SELECT * FROM see_cagg;

-- This is the simplest case, when the updated bucket is inside a chunk, so it is expected
-- that the update is always reflected after the refresh on drop.
UPDATE conditions SET value = 4.00 WHERE time_int = 2;

-- This case updates values in the bucket, which affects two different partials in
-- two different chunks, which are dropped in the same call to drop_chunks.
UPDATE conditions SET value = 4.00 WHERE time_int = 9;
UPDATE conditions SET value = 4.00 WHERE time_int = 11;

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as 
    $$ SELECT 120 $$;


SELECT drop_chunks(20, 'conditions', cascade_to_materializations => false);
SELECT * FROM see_cagg;

UPDATE conditions SET value = 4.00 WHERE time_int = 20;
UPDATE conditions SET value = 4.00 WHERE time_int = 39;

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as 
    $$ SELECT 140 $$;
SELECT drop_chunks(40, 'conditions', cascade_to_materializations => false);
SELECT * FROM see_cagg;

UPDATE conditions SET value = 4.00 WHERE time_int = 41;

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as 
    $$ SELECT 160 $$;

SELECT drop_chunks(60, 'conditions', cascade_to_materializations => false);
SELECT * FROM see_cagg;

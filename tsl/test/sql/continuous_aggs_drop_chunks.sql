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
        FROM records GROUP BY bucket, clientId;

INSERT INTO clients(name) VALUES ('test-client');

INSERT INTO records
SELECT generate_series('2000-03-01'::timestamptz,'2000-04-01','1 day'),1,3.14;

SET timescaledb.current_timestamp_mock = '2000-04-01';

SELECT * FROM records_monthly;

ALTER MATERIALIZED VIEW records_monthly SET (
   timescaledb.ignore_invalidation_older_than = '15 days'
);

SELECT chunk_name, range_start, range_end 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'records_monthly' ORDER BY range_start;

SELECT chunk_name, range_start, range_end 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'records' ORDER BY range_start;

REFRESH MATERIALIZED VIEW records_monthly;
REFRESH MATERIALIZED VIEW records_monthly;

\set VERBOSITY default
SELECT drop_chunks('records', '2000-03-16'::timestamptz);


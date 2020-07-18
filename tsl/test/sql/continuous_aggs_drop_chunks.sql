-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

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
SELECT drop_chunks('records', '2000-03-16'::timestamptz,
       cascade_to_materializations => FALSE);

-- Issue #1921 (https://github.com/timescale/timescaledb/issues/1921)
-- drop_chunks() Fails against hypertable with cagg defined with no ignore_invalidation_older set 
CREATE TABLE conditions (
    time       TIMESTAMPTZ       NOT NULL,
    temperature DOUBLE PRECISION  NULL
);

SELECT table_name FROM create_hypertable( 'conditions', 'time');

CREATE VIEW conditions_cagg( timec, maxt)
    WITH ( timescaledb.continuous, timescaledb.refresh_lag='-1day',
        timescaledb.max_interval_per_job='260 day')
AS
SELECT time_bucket('1day', time), max(temperature)
FROM conditions
GROUP BY time_bucket('1day', time);

INSERT INTO conditions VALUES ('2020-06-10', '1');
INSERT INTO conditions VALUES ('2020-06-10', '2');
INSERT INTO conditions VALUES ('2020-05-15', '3');
INSERT INTO conditions VALUES ('2020-05-15', '6');
INSERT INTO conditions VALUES ('2020-04-23', '10');
INSERT INTO conditions VALUES ('2020-04-23', '11');

SET timescaledb.current_timestamp_mock = '2020-07-01';

REFRESH MATERIALIZED VIEW conditions_cagg;
SELECT * FROM conditions_cagg;
-- No error on this statement
SELECT drop_chunks('conditions', '2020-06-01'::timestamptz, cascade_to_materializations => FALSE);
-- No changes to cagg since cascade_to_materialization is FALSE
SELECT * FROM conditions_cagg;



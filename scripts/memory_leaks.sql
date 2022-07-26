-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
\c test
\timing

DROP EXTENSION IF EXISTS timescaledb CASCADE;
CREATE EXTENSION timescaledb;

SELECT pg_backend_pid();
SELECT pg_sleep(5);

CREATE TABLE metrics(time timestamptz, device text, value float);
SELECT create_hypertable('metrics', 'time');

INSERT INTO metrics SELECT '2020-01-01'::timestamptz + format('%ss',time)::interval, format('Device %s',time%10), random() FROM generate_series(1,1000000) g(time);


\o /dev/null
\set ECHO none
\timing off
\echo Starting 1 million SELECT queries
SELECT format($$SELECT * FROM metrics WHERE device = 'Device %s' ORDER BY time DESC LIMIT 1;$$,time % 10) FROM generate_series(1,1000000) g(time) \gexec

\echo Starting 10k UPDATE queries
SELECT format($$UPDATE metrics SET value = %s WHERE time < '2020-01-02' AND device = 'Device %s';$$,time,time % 10) FROM generate_series(1,10000) g(time) \gexec

\echo Starting 50k INSERTs with batches of 1000
SELECT format($$INSERT INTO metrics SELECT '2021-01-01'::timestamptz + '%ss'::interval + format('%%s', time)::interval, format('Device %%s',time %% 10), random() FROM generate_series(1,1000) g(time);$$,time) FROM generate_series(1,50000) g(time) \gexec


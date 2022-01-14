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

CREATE TABLE test_hyper (i bigint, j double precision, k bigint, ts timestamp);
SELECT create_hypertable('test_hyper', 'i', chunk_time_interval=>10);

INSERT INTO test_hyper SELECT random()*100, random()*100, x*10, _timescaledb_internal.to_timestamp(x)  FROM generate_series(1,100000000) AS x;

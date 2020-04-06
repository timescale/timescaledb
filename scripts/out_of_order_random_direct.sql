-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

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

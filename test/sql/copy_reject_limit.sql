-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- REJECT_LIMIT was introduced in PostgreSQL 18.
CREATE TABLE copy_limit (time bigint NOT NULL, val int);
SELECT create_hypertable('copy_limit', 'time', chunk_time_interval => 10);

-- Exactly the limit is allowed, including errors in the partition column.
COPY copy_limit FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 2);
1,1
bad_time,2
11,3
21,bad_value
31,5
\.
SELECT * FROM copy_limit ORDER BY time;

-- The counter starts over for each COPY statement.
COPY copy_limit FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1);
41,bad_value
51,7
\.
SELECT * FROM copy_limit ORDER BY time;

-- Exceeding the limit aborts COPY and rolls back all of that statement's rows
-- and chunk creation, preserving the rows from earlier successful statements.
\set ON_ERROR_STOP 0
\set VERBOSITY terse
COPY copy_limit FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1);
61,9
71,bad_value
81,11
bad_time,13
91,15
\.
SELECT :'SQLSTATE' AS copy_sqlstate;
\set ON_ERROR_STOP 1
\set VERBOSITY default
SELECT * FROM copy_limit ORDER BY time;
SELECT count(*) AS chunks FROM show_chunks('copy_limit');

-- Also roll back rows already inserted via the unbuffered path. Silent logging
-- suppresses notices, but must not suppress the rejection-limit error.
SET timescaledb.enable_optimizations = off;
\set ON_ERROR_STOP 0
\set VERBOSITY terse
COPY copy_limit FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1, LOG_VERBOSITY silent);
61,9
71,bad_value
81,11
bad_time,13
91,15
\.
SELECT :'SQLSTATE' AS copy_sqlstate;
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET timescaledb.enable_optimizations;
SELECT * FROM copy_limit ORDER BY time;
SELECT count(*) AS chunks FROM show_chunks('copy_limit');

DROP TABLE copy_limit;

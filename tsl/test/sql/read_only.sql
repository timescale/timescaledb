-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Following tests checks that API functions which modify data (including catalog)
-- properly recognize read-only transaction state
--

-- create_hypertable()
--
CREATE TABLE test_table(time bigint NOT NULL, device int);

-- EXPLAIN should work in read-only mode, when enabling in transaction.
START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (COSTS OFF) SELECT * FROM test_table;
ROLLBACK;

START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM test_table;
ROLLBACK;

START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (COSTS OFF) INSERT INTO test_table VALUES (1, 1);
ROLLBACK;

-- This should give an error since we are using ANALYZE and a DML. The
-- read-only is checked when executing the actual INSERT statement,
-- not inside our code.
\set ON_ERROR_STOP 0
START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF, SUMMARY OFF) INSERT INTO test_table VALUES (1, 1);
ROLLBACK;
\set ON_ERROR_STOP 1

SET default_transaction_read_only TO on;

-- EXPLAIN should work in read-only mode, even when using the default.
START TRANSACTION;
EXPLAIN (COSTS OFF) SELECT * FROM test_table;
ROLLBACK;

START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM test_table;
ROLLBACK;

START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (COSTS OFF) INSERT INTO test_table VALUES (1, 1);
ROLLBACK;

-- This should give an error since we are using ANALYZE and a DML. The
-- read-only is checked when executing the actual INSERT statement,
-- not inside our code.
\set ON_ERROR_STOP 0
START TRANSACTION;
SET transaction_read_only TO on;
EXPLAIN (ANALYZE ON, COSTS OFF, TIMING OFF, SUMMARY OFF) INSERT INTO test_table VALUES (1, 1);
ROLLBACK;
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('test_table', 'time');
\set ON_ERROR_STOP 1

SET default_transaction_read_only TO off;
SELECT * FROM create_hypertable('test_table', 'time', chunk_time_interval => 1000000::bigint);

SET default_transaction_read_only TO on;

\set ON_ERROR_STOP 0

-- set_chunk_time_interval()
--
SELECT * FROM set_chunk_time_interval('test_table', 2000000000::bigint);

-- set_number_partitions()
--
SELECT * FROM set_number_partitions('test_table', 2);

-- set_adaptive_chunking()
--
SELECT * FROM set_adaptive_chunking('test_table', '2MB');

-- drop_chunks()
--
SELECT * FROM drop_chunks('test_table', older_than => 10);

-- add_dimension()
--
SELECT * FROM add_dimension('test_table', 'device', chunk_time_interval => 100);

\set ON_ERROR_STOP 1

-- tablespaces
--
SET default_transaction_read_only TO off;

SET client_min_messages TO error;
DROP TABLESPACE IF EXISTS tablespace1;
RESET client_min_messages;
CREATE TABLESPACE tablespace1 OWNER :ROLE_CLUSTER_SUPERUSER LOCATION :TEST_TABLESPACE1_PATH;

SET default_transaction_read_only TO on;

-- attach_tablespace()
--
\set ON_ERROR_STOP 0
SELECT * FROM attach_tablespace('tablespace1', 'test_table');
\set ON_ERROR_STOP 1

SET default_transaction_read_only TO off;
SELECT * FROM attach_tablespace('tablespace1', 'test_table');

-- detach_tablespace()
--
SET default_transaction_read_only TO on;
\set ON_ERROR_STOP 0
SELECT * FROM detach_tablespace('tablespace1', 'test_table');
\set ON_ERROR_STOP 1

-- detach_tablespaces()
--
\set ON_ERROR_STOP 0
SELECT * FROM detach_tablespaces('test_table');
\set ON_ERROR_STOP 1
SET default_transaction_read_only TO off;
SELECT * FROM detach_tablespaces('test_table');

DROP TABLESPACE tablespace1;

-- drop hypertable
--
SET default_transaction_read_only TO on;
\set ON_ERROR_STOP 0
DROP TABLE test_table;
\set ON_ERROR_STOP 1
SET default_transaction_read_only TO off;
DROP TABLE test_table;

-- Test some read-only cases of DDL operations
--
CREATE TABLE test_table(time bigint NOT NULL, device int);
SELECT * FROM create_hypertable('test_table', 'time', chunk_time_interval => 1000000::bigint);
INSERT INTO test_table VALUES (0, 1), (1, 1), (2, 2);

SET default_transaction_read_only TO on;

-- CREATE INDEX
--
\set ON_ERROR_STOP 0
CREATE INDEX test_table_device_idx ON test_table(device);
\set ON_ERROR_STOP 1

-- TRUNCATE
--
\set ON_ERROR_STOP 0
TRUNCATE test_table;
\set ON_ERROR_STOP 1

-- ALTER TABLE
--
\set ON_ERROR_STOP 0
ALTER TABLE test_table DROP COLUMN device;
ALTER TABLE test_table ADD CONSTRAINT device_check CHECK (device > 0);
\set ON_ERROR_STOP 1

-- VACUUM
--
\set ON_ERROR_STOP 0
VACUUM test_table;
\set ON_ERROR_STOP 1

-- CLUSTER
--
\set ON_ERROR_STOP 0
CLUSTER test_table USING test_table_time_idx;
\set ON_ERROR_STOP 1

-- COPY FROM
--
\set ON_ERROR_STOP 0
COPY test_table (time, device) FROM STDIN DELIMITER ',';
\set ON_ERROR_STOP 1

-- COPY TO (expect to be working in read-only mode)
--
COPY (SELECT * FROM test_Table ORDER BY time) TO STDOUT;

-- Test Continuous Aggregates
--
SET default_transaction_read_only TO off;

CREATE TABLE test_contagg (
  observation_time  TIMESTAMPTZ       NOT NULL,
  device_id         TEXT              NOT NULL,
  metric            DOUBLE PRECISION  NOT NULL,
  PRIMARY KEY(observation_time, device_id)
);

SELECT create_hypertable('test_contagg', 'observation_time');

SET default_transaction_read_only TO on;

-- CREATE VIEW
--
\set ON_ERROR_STOP 0

CREATE MATERIALIZED VIEW test_contagg_view
WITH (timescaledb.continuous)
AS
SELECT
  time_bucket('1 hour', observation_time) as bucket,
  device_id,
  avg(metric) as metric_avg,
  max(metric)-min(metric) as metric_spread
FROM
  test_contagg
GROUP BY bucket, device_id WITH NO DATA;

-- policy API
-- compression policy will not throw an error, as it is expected to continue
-- with next chunks
SET default_transaction_read_only TO off;
CREATE TABLE test_table_int(time bigint NOT NULL, device int);
SELECt create_hypertable('test_table_int', 'time', chunk_time_interval=>'1'::bigint);
create or replace function dummy_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT';
select set_integer_now_func('test_table_int', 'dummy_now');
ALTER TABLE test_table_int SET (timescaledb.compress);
INSERT INTO test_table_int VALUES (0, 1), (10,10);
SELECT add_compression_policy('test_table_int', '1'::integer) as comp_job_id \gset
SELECT config as comp_job_config
FROM _timescaledb_config.bgw_job WHERE id = :comp_job_id \gset
SET default_transaction_read_only TO on;
CALL _timescaledb_functions.policy_compression(:comp_job_id, :'comp_job_config');
SET default_transaction_read_only TO off;
--verify chunks are not compressed
SELECT count(*) , count(*) FILTER ( WHERE is_compressed is true)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'test_table_int';
--cleanup
DROP TABLE test_table_int;

SET default_transaction_read_only TO on;
CALL _timescaledb_functions.policy_refresh_continuous_aggregate(1,'{}');
CALL _timescaledb_functions.policy_reorder(1,'{}');
CALL _timescaledb_functions.policy_retention(1,'{}');

SELECT add_compression_policy('test_table', '1w');
SELECT remove_compression_policy('test_table');

SELECT add_reorder_policy('test_table', 'test_table_time_idx');
SELECT remove_reorder_policy('test_table');

SELECT add_retention_policy('test_table', '1w');
SELECT remove_retention_policy('test_table');

SELECT add_job('now','12h');
SELECT alter_job(1,scheduled:=false);
SELECT delete_job(1);


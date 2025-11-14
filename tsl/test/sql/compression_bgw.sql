-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE ROLE NOLOGIN_ROLE WITH nologin noinherit;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO NOLOGIN_ROLE;
GRANT NOLOGIN_ROLE TO :ROLE_DEFAULT_PERM_USER WITH ADMIN OPTION;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timezone TO 'America/Los_Angeles';

CREATE TABLE conditions (
      time        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      location2    char(10)              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );
select create_hypertable( 'conditions', 'time', chunk_time_interval=> '31days'::interval);

--TEST 1--
--cannot set policy without enabling compression --
\set ON_ERROR_STOP 0
select add_compression_policy('conditions', '60d'::interval);
\set ON_ERROR_STOP 1

-- TEST2 --
--add a policy to compress chunks --
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'time');
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;

select add_compression_policy('conditions', '60d'::interval) AS compressjob_id
\gset

select * from _timescaledb_config.bgw_job where id = :compressjob_id;
select * from alter_job(:compressjob_id, schedule_interval=>'1s');
--enable maxchunks to 1 so that only 1 chunk is compressed by the job
SELECT alter_job(id,config:=jsonb_set(config,'{maxchunks_to_compress}', '1'))
 FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

select * from _timescaledb_config.bgw_job where id >= 1000 ORDER BY id;
insert into conditions
select now()::timestamp, 'TOK', 'sony', 55, 75;

-- TEST3 --
--only the old chunks will get compressed when policy is executed--
CALL run_job(:compressjob_id);
select chunk_name, pg_size_pretty(before_compression_total_bytes) before_total,
pg_size_pretty( after_compression_total_bytes)  after_total
from chunk_compression_stats('conditions') where compression_status like 'Compressed' order by chunk_name;
SELECT id, hypertable_id, schema_name, table_name, compressed_chunk_id, dropped, status, osm_chunk FROM _timescaledb_catalog.chunk ORDER BY id;

-- TEST 4 --
--cannot set another policy
\set ON_ERROR_STOP 0
select add_compression_policy('conditions', '60d'::interval, if_not_exists=>true);
select add_compression_policy('conditions', '60d'::interval);
select add_compression_policy('conditions', '30d'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1

--TEST 5 --
-- drop the policy --
select remove_compression_policy('conditions');
select count(*) from _timescaledb_config.bgw_job WHERE id>=1000;

--TEST 6 --
-- try to execute the policy after it has been dropped --
\set ON_ERROR_STOP 0
CALL run_job(:compressjob_id);

--errors with bad input for add/remove compression policy
create view dummyv1 as select * from conditions limit 1;
select add_compression_policy( 100 , compress_after=> '1 day'::interval);
select add_compression_policy( 'dummyv1', compress_after=> '1 day'::interval );
select remove_compression_policy( 100 );
\set ON_ERROR_STOP 1

-- We're done with the table, so drop it.
DROP TABLE IF EXISTS conditions CASCADE;

--TEST 7
--compression policy for smallint, integer or bigint based partition hypertable
--smallint test
CREATE TABLE test_table_smallint(time SMALLINT, val SMALLINT);
SELECT create_hypertable('test_table_smallint', 'time', chunk_time_interval => 1);

CREATE OR REPLACE FUNCTION dummy_now_smallint() RETURNS SMALLINT LANGUAGE SQL IMMUTABLE AS 'SELECT 5::SMALLINT';
SELECT set_integer_now_func('test_table_smallint', 'dummy_now_smallint');

INSERT INTO test_table_smallint SELECT generate_series(1,5), 10;

ALTER TABLE test_table_smallint SET (timescaledb.compress);
\set ON_ERROR_STOP 0
select add_compression_policy( 'test_table_smallint', compress_after=> '1 day'::interval );
\set ON_ERROR_STOP 1
SELECT add_compression_policy('test_table_smallint', 2::SMALLINT) AS compressjob_id \gset
SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

--will compress all chunks that need compression
CALL run_job(:compressjob_id);

SELECT chunk_name, before_compression_total_bytes, after_compression_total_bytes
FROM chunk_compression_stats('test_table_smallint')
WHERE compression_status LIKE 'Compressed'
ORDER BY chunk_name;

--integer tests
CREATE TABLE test_table_integer(time INTEGER, val INTEGER);
SELECT create_hypertable('test_table_integer', 'time', chunk_time_interval => 1);

CREATE OR REPLACE FUNCTION dummy_now_integer() RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS 'SELECT 5::INTEGER';
SELECT set_integer_now_func('test_table_integer', 'dummy_now_integer');

INSERT INTO test_table_integer SELECT generate_series(1,5), 10;

ALTER TABLE test_table_integer SET (timescaledb.compress);
SELECT add_compression_policy('test_table_integer', 2::INTEGER) AS compressjob_id \gset
SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

--will compress all chunks that need compression
CALL run_job(:compressjob_id);

SELECT chunk_name, before_compression_total_bytes, after_compression_total_bytes
FROM chunk_compression_stats('test_table_integer')
WHERE compression_status LIKE 'Compressed'
ORDER BY chunk_name;

--bigint test
CREATE TABLE test_table_bigint(time BIGINT, val BIGINT);
SELECT create_hypertable('test_table_bigint', 'time', chunk_time_interval => 1);

CREATE OR REPLACE FUNCTION dummy_now_bigint() RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS 'SELECT 5::BIGINT';
SELECT set_integer_now_func('test_table_bigint', 'dummy_now_bigint');

INSERT INTO test_table_bigint SELECT generate_series(1,5), 10;

ALTER TABLE test_table_bigint SET (timescaledb.compress);
SELECT add_compression_policy('test_table_bigint', 2::BIGINT) AS compressjob_id \gset
SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

--will compress all chunks that need compression
CALL run_job(:compressjob_id);

SELECT chunk_name, before_compression_total_bytes, after_compression_total_bytes
FROM chunk_compression_stats('test_table_bigint')
WHERE compression_status LIKE 'Compressed'
ORDER BY chunk_name;

--TEST 8
--hypertable owner lacks permission to start background worker
SET ROLE NOLOGIN_ROLE;
CREATE TABLE test_table_nologin(time bigint, val int);
SELECT create_hypertable('test_table_nologin', 'time', chunk_time_interval => 1);
SELECT set_integer_now_func('test_table_nologin', 'dummy_now_bigint');
ALTER TABLE test_table_nologin set (timescaledb.compress);
\set ON_ERROR_STOP 0
SELECT add_compression_policy('test_table_nologin', 2::int);
\set ON_ERROR_STOP 1
DROP TABLE test_table_nologin;
RESET ROLE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timezone TO 'America/Los_Angeles';

CREATE TABLE conditions(
    time TIMESTAMPTZ NOT NULL,
    device INTEGER,
    temperature FLOAT
);
SELECT * FROM create_hypertable('conditions', 'time',
                                chunk_time_interval => '1 day'::interval);

INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '10 min') AS time;

CREATE MATERIALIZED VIEW conditions_summary
WITH (timescaledb.continuous) AS
SELECT device,
       time_bucket(INTERVAL '1 hour', "time") AS day,
       AVG(temperature) AS avg_temperature,
       MAX(temperature) AS max_temperature,
       MIN(temperature) AS min_temperature
FROM conditions
GROUP BY device, time_bucket(INTERVAL '1 hour', "time") WITH NO DATA;

CALL refresh_continuous_aggregate('conditions_summary', NULL, NULL);

ALTER TABLE conditions SET (timescaledb.compress);

SELECT COUNT(*) AS dropped_chunks_count
  FROM drop_chunks('conditions', TIMESTAMPTZ '2018-12-15 00:00');

SELECT count(*) FROM timescaledb_information.chunks
WHERE hypertable_name = 'conditions' and is_compressed = true;

SELECT add_compression_policy AS job_id
  FROM add_compression_policy('conditions', INTERVAL '1 day') \gset
-- job compresses only 1 chunk at a time --
SELECT alter_job(id,config:=jsonb_set(config,'{maxchunks_to_compress}', '1'))
 FROM _timescaledb_config.bgw_job WHERE id = :job_id;
SELECT alter_job(id,config:=jsonb_set(config,'{verbose_log}', 'true'))
 FROM _timescaledb_config.bgw_job WHERE id = :job_id;
set client_min_messages TO LOG;
CALL run_job(:job_id);
set client_min_messages TO NOTICE;

SELECT count(*) FROM timescaledb_information.chunks
WHERE hypertable_name = 'conditions' and is_compressed = true;

\i include/recompress_basic.sql

--TEST 7
--compression policy should ignore frozen partially compressed chunks
CREATE TABLE test_table_frozen(time TIMESTAMPTZ, val SMALLINT);
SELECT create_hypertable('test_table_frozen', 'time', chunk_time_interval => '1 day'::interval);

INSERT INTO test_table_frozen SELECT time, (random()*10)::smallint
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '10 min') AS time;

ALTER TABLE test_table_frozen SET (timescaledb.compress);
select add_compression_policy( 'test_table_frozen', compress_after=> '1 day'::interval ) as compressjob_id \gset
SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;
SELECT show_chunks('test_table_frozen') as first_chunk LIMIT 1 \gset

--will compress all chunks that need compression
CALL run_job(:compressjob_id);

-- make the chunks partial
INSERT INTO test_table_frozen SELECT time, (random()*10)::smallint
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '10 min') AS time;

SELECT c.id, c.status
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_table_frozen'
ORDER BY c.id
LIMIT 1;

-- freeze first chunk
SELECT _timescaledb_functions.freeze_chunk(:'first_chunk');

-- first chunk status is  1 (Compressed) + 8 (Partially compressed) + 4 (Frozen) = 13
SELECT c.id, c.status
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_table_frozen'
ORDER BY c.id
LIMIT 1;

--should recompress all chunks except first since its frozen
CALL run_job(:compressjob_id);

-- first chunk status is unchanged
SELECT c.id, c.status
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_table_frozen'
ORDER BY c.id
LIMIT 1;

-- unfreeze first chunk
SELECT _timescaledb_functions.unfreeze_chunk(:'first_chunk');

-- should be able to recompress the chunk since its unfrozen
CALL run_job(:compressjob_id);

-- first chunk status is Compressed (1)
SELECT c.id, c.status
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_table_frozen'
ORDER BY c.id
LIMIT 1;

--TEST 8
--reindexing in recompression policy
CREATE TABLE metrics2(time DATE NOT NULL);
CREATE INDEX metrics2_index ON metrics2(time DESC);
SELECT hypertable_id AS "HYPERTABLE_ID", schema_name, table_name, created FROM create_hypertable('metrics2','time') \gset
ALTER TABLE metrics2 SET (timescaledb.compress);
INSERT INTO metrics2 SELECT generate_series('2000-01-01'::date, '2000-02-01'::date, '5m'::interval);

SELECT add_job('_timescaledb_functions.policy_compression','1w',('{"hypertable_id": '||:'HYPERTABLE_ID'||', "compress_after": "@ 7 days"}')::jsonb, initial_start => '2000-01-01 00:00:00+00'::timestamptz) AS "JOB_COMPRESS" \gset

-- first call should compress
CALL run_job(:JOB_COMPRESS);

-- status should be 1
SELECT chunk_status FROM compressed_chunk_info_view WHERE hypertable_name = 'metrics2';

-- disable reindex in compress job
SELECT alter_job(id,config:=jsonb_set(config,'{reindex}','false'), next_start => '2000-01-01 00:00:00+00'::timestamptz) FROM _timescaledb_config.bgw_job WHERE id = :JOB_COMPRESS;

-- do an INSERT so recompress has something to do
INSERT INTO metrics2 SELECT '2000-01-01' FROM generate_series(1,3000);

SELECT chunk_schema, chunk_name FROM compressed_chunk_info_view WHERE hypertable_name = 'metrics2' AND chunk_status = 9 LIMIT 1; \gset
SELECT format('%I.%I', :'chunk_schema', :'chunk_name') AS "RECOMPRESS_CHUNK_NAME"; \gset

-- get size of the chunk that needs recompression
VACUUM ANALYZE :RECOMPRESS_CHUNK_NAME;

SELECT pg_indexes_size(:'RECOMPRESS_CHUNK_NAME') AS "SIZE_BEFORE_REINDEX"; \gset

CALL run_job(:JOB_COMPRESS);

-- status should be 1
SELECT chunk_status FROM compressed_chunk_info_view WHERE chunk_schema = :'chunk_schema' AND chunk_name = :'chunk_name';

-- index size should not have decreased
VACUUM ANALYZE :RECOMPRESS_CHUNK_NAME;

-- index size can vary, vacuuming can even increase the size of the index
-- just check that the index size hasn't decreased, this can only happen
-- when running VACUUM FULL or REINDEX TABLE
SELECT pg_indexes_size(:'RECOMPRESS_CHUNK_NAME') >= :SIZE_BEFORE_REINDEX as size_unchanged;

-- enable reindex in compress job
SELECT alter_job(id,config:=jsonb_set(config,'{reindex}','true'), next_start => '2000-01-01 00:00:00+00'::timestamptz) FROM _timescaledb_config.bgw_job WHERE id = :JOB_COMPRESS;

-- do an INSERT so recompress has something to do
INSERT INTO metrics2 SELECT '2000-01-01';

---- status should be 3
SELECT chunk_status FROM compressed_chunk_info_view WHERE chunk_schema = :'chunk_schema' AND chunk_name = :'chunk_name';

-- should recompress
CALL run_job(:JOB_COMPRESS);

-- index size should decrease due to reindexing (8kB or 16kB)
VACUUM ANALYZE metrics2;
SELECT pg_indexes_size(:'RECOMPRESS_CHUNK_NAME') <= 16384 as size_empty;
DROP TABLE metrics2;

--TEST 8
--compression policy errors
CREATE TABLE test_compression_policy_errors(time TIMESTAMPTZ, val SMALLINT);
SELECT create_hypertable('test_compression_policy_errors', 'time', chunk_time_interval => '1 day'::interval);
ALTER TABLE test_compression_policy_errors SET (timescaledb.compress, timescaledb.compress_segmentby = 'val', timescaledb.compress_orderby = 'time');

INSERT INTO test_compression_policy_errors SELECT time, (random()*10)::smallint
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '10 min') AS time;

SELECT
  add_compression_policy(
    'test_compression_policy_errors',
    compress_after=> '1 day'::interval,
    initial_start => now() - interval '1 day'
  ) as compressjob_id \gset

SELECT config AS compressjob_config FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id \gset
SELECT FROM alter_job(:compressjob_id, config => jsonb_set(:'compressjob_config'::jsonb, '{recompress}', 'true'));

-- 31 uncompressed chunks (0 - uncompressed, 1 - compressed)
SELECT c.status, count(*)
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_compression_policy_errors'
GROUP BY c.status
ORDER BY 2 DESC;

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Let's mess with the chunk status to for an error when executing the job
WITH chunks AS (
  SELECT c.id, c.status
  FROM _timescaledb_catalog.chunk c
  INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
  WHERE h.table_name = 'test_compression_policy_errors'
  ORDER BY c.id LIMIT 20
)
UPDATE _timescaledb_catalog.chunk
SET status = 3
FROM chunks
WHERE chunk.id = chunks.id
  AND chunk.status = 0;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- After the mess 20 = status 3 and 11 = status 0
SELECT c.status, count(*)
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_compression_policy_errors'
GROUP BY c.status
ORDER BY 2 DESC;

\set ON_ERROR_STOP 0
SET client_min_messages TO ERROR;
\set VERBOSITY default
-- This should fail with
-- 20 chunks failed to compress and 11 chunks compressed successfully
CALL run_job(:compressjob_id);
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- 31 uncompressed chunks (0 - uncompressed, 1 - compressed)
SELECT c.status, count(*)
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
WHERE h.table_name = 'test_compression_policy_errors'
GROUP BY c.status
ORDER BY 2 DESC;

-- Teardown test
\c :TEST_DBNAME :ROLE_SUPERUSER
REVOKE CREATE ON SCHEMA public FROM NOLOGIN_ROLE;
DROP ROLE NOLOGIN_ROLE;

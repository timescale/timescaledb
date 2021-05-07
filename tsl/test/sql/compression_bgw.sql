-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE ROLE NOLOGIN_ROLE WITH nologin noinherit;
GRANT NOLOGIN_ROLE TO :ROLE_DEFAULT_PERM_USER WITH ADMIN OPTION;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

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
select * from _timescaledb_config.bgw_job where id >= 1000 ORDER BY id;
insert into conditions
select now()::timestamp, 'TOK', 'sony', 55, 75;

-- TEST3 --
--only the old chunks will get compressed when policy is executed--
CALL run_job(:compressjob_id);
select chunk_name, pg_size_pretty(before_compression_total_bytes) before_total,
pg_size_pretty( after_compression_total_bytes)  after_total
from chunk_compression_stats('conditions') where compression_status like 'Compressed' order by chunk_name;
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;

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
\set ON_ERROR_STOP 1

-- We're done with the table, so drop it.
DROP TABLE IF EXISTS conditions CASCADE;

--TEST 7
--compression policy for integer based partition hypertable
CREATE TABLE test_table_int(time bigint, val int);
SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 1);

create or replace function dummy_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT';
select set_integer_now_func('test_table_int', 'dummy_now');
insert into test_table_int select generate_series(1,5), 10;
alter table test_table_int set (timescaledb.compress);
select add_compression_policy('test_table_int', 2::int) AS compressjob_id
\gset

select * from _timescaledb_config.bgw_job where id=:compressjob_id;
\gset
CALL run_job(:compressjob_id);
CALL run_job(:compressjob_id);
select chunk_name, before_compression_total_bytes, after_compression_total_bytes
from chunk_compression_stats('test_table_int') where compression_status like 'Compressed' order by chunk_name;

--TEST 8
--hypertable owner lacks permission to start background worker
SET ROLE NOLOGIN_ROLE;
CREATE TABLE test_table_nologin(time bigint, val int);
SELECT create_hypertable('test_table_nologin', 'time', chunk_time_interval => 1);
SELECT set_integer_now_func('test_table_nologin', 'dummy_now');
ALTER TABLE test_table_nologin set (timescaledb.compress);
\set ON_ERROR_STOP 0
SELECT add_compression_policy('test_table_nologin', 2::int);
\set ON_ERROR_STOP 1
RESET ROLE;
REVOKE NOLOGIN_ROLE FROM :ROLE_DEFAULT_PERM_USER;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

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

-- We need to have some chunks that are marked as dropped, otherwise
-- we will not have a problem below.
SELECT COUNT(*) AS dropped_chunks_count
  FROM _timescaledb_catalog.chunk
 WHERE dropped = TRUE;

SELECT add_compression_policy AS job_id
  FROM add_compression_policy('conditions', INTERVAL '1 day') \gset
CALL run_job(:job_id);

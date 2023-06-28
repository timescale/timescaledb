\ir ../../../test/sql/include/test_utils.sql

CREATE TABLE main_table AS
SELECT '2011-11-11 11:11:11'::timestamptz AS time, 'foo' AS device_id limit 0;

SELECT create_hypertable('main_table', 'time', chunk_time_interval => interval '12 hour', migrate_data => TRUE);

ALTER TABLE main_table SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = '');

INSERT INTO main_table SELECT t, 'dev1'  FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-05 3:00', '1 hour') t;

select show_chunks('main_table');

SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('main_table')
ORDER BY slices;

with t as (SELECT * FROM show_chunks('main_table') as t(ch) order by ch)
select (select ch from t limit 1 offset 2) as chunk, (select ch from t limit 1 offset 3) as chunk2 \gset

-- select '_timescaledb_internal._hyper_1_3_chunk' as chunk \gset
-- select '_timescaledb_internal._hyper_1_4_chunk' as chunk2 \gset

select _timescaledb_internal.show_chunk(:'chunk');
select _timescaledb_internal.show_chunk(:'chunk2');

select count(1) from :chunk;
select count(1) from main_table;
\d :chunk

-- reject detach if compressed
select compress_chunk(:'chunk'::regclass);
\set ON_ERROR_STOP 0
select _timescaledb_internal.chunk_detach(:'chunk'::regclass);
\set ON_ERROR_STOP 1
select decompress_chunk(:'chunk'::regclass);

-- reject if cagg is present
CREATE MATERIALIZED VIEW mat_m1(a, countb) WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.finalized=false)
as select time_bucket('1 hour', time),device_id, count(1) from main_table group by time_bucket('1 hour', time), device_id WITH NO DATA;
\set ON_ERROR_STOP 0
select _timescaledb_internal.chunk_detach(:'chunk'::regclass);
\set ON_ERROR_STOP 1
DROP MATERIALIZED VIEW mat_m1;

select _timescaledb_internal.chunk_detach(:'chunk'::regclass);

select assert_equal(count(1),12::bigint) from :chunk;
select assert_equal(count(1),63::bigint) from main_table;
\d :chunk

select _timescaledb_internal.chunk_detach(:'chunk2'::regclass);

create table n (like main_table);

\d n
insert into n 
select * from :chunk union all select * from :chunk2;

select _timescaledb_internal.chunk_attach('main_table','{"time": [1520035200000000, 1520121600000000]}'::jsonb,'n'::regclass);

select assert_equal(count(1),75::bigint) from main_table;

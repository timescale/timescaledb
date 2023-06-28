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
select
    (select ch from t limit 1 offset 2) as chunk1,
    (select ch from t limit 1 offset 3) as chunk2,
    (select ch from t limit 1 offset 4) as chunk3
    \gset

select slices as slice1 from _timescaledb_internal.show_chunk(:'chunk1') \gset
select slices as slice2 from _timescaledb_internal.show_chunk(:'chunk2') \gset
select slices as slice3 from _timescaledb_internal.show_chunk(:'chunk3') \gset

select assert_equal(count(1),75::bigint) from main_table;

\d :chunk1
\d :chunk2

select assert_equal(count(1),75::bigint) from main_table;

-- invalid range must fail
\set ON_ERROR_STOP 0
select chunk_merge('main_table', :'chunk1', :'chunk3');
\set ON_ERROR_STOP 1

select chunk_merge('main_table', :'chunk1', :'chunk2');

select assert_equal(count(1),75::bigint) from main_table;

\d+ main_table

with t as (SELECT * FROM show_chunks('main_table') as t(ch) order by ch desc)
select (select ch from t limit 1) as new_chunk \gset

select * from _timescaledb_internal.show_chunk(:'new_chunk');

\d :new_chunk

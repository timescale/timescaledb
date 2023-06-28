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

select slices as slice1 from _timescaledb_internal.show_chunk(:'chunk') \gset
select slices as slice2 from _timescaledb_internal.show_chunk(:'chunk2') \gset

select assert_equal(count(1),75::bigint) from main_table;

select chunk_merge(main_table, :chunk, :chunk2);

select assert_equal(count(1),75::bigint) from main_table;

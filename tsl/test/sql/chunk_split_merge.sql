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

CREATE OR REPLACE FUNCTION chunk_merge(hypertable REGCLASS, chunk1 REGCLASS,chunk2 REGCLASS) RETURNS jsonb
AS
$BODY$
DECLARE
    merged_slice  jsonb;
BEGIN
    select slices into merged_slice
        from _timescaledb_internal.chunk_detach(chunk1) as t(slices);
    RAISE NOTICE 'asd %',merged_slice;
    select _timescaledb_internal.slice_union(hypertable,slices,merged_slice) into merged_slice
        from _timescaledb_internal.chunk_detach(chunk2) as t(slices);
    RAISE NOTICE 'asd %',merged_slice;

    EXECUTE format('create table new_1 ( like %s )',hypertable);
    EXECUTE format('insert into new_1 select * from %s union all select * from %s',chunk1,chunk2);
    
    perform _timescaledb_internal.chunk_attach(hypertable,merged_slice, 'new_1');

    RETURN merged_slice;
END;
$BODY$
LANGUAGE PLPGSQL VOLATILE;

\d :chunk1
\d :chunk2

select assert_equal(count(1),75::bigint) from main_table;

select chunk_merge('main_table', :'chunk1', :'chunk2');

select assert_equal(count(1),75::bigint) from main_table;

\d+ main_table

with t as (SELECT * FROM show_chunks('main_table') as t(ch) order by ch desc)
select (select ch from t limit 1) as new_chunk \gset

\d :new_chunk

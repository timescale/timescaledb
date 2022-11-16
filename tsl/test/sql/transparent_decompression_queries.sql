-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--TEST github issue 1650 character segment by column
CREATE TABLE test_chartab ( job_run_id INTEGER NOT NULL, mac_id CHAR(16) NOT NULL, rtt INTEGER NOT NULL, ts TIMESTAMP(3) NOT NULL );

SELECT create_hypertable('test_chartab', 'ts', chunk_time_interval => interval '1 day', migrate_data => true);
insert into test_chartab
values(8864, '0014070000006190' , 392 , '2019-12-14 02:52:05.863');
insert into test_chartab
values( 8864 , '0014070000011039' , 150 , '2019-12-14 02:52:05.863');
insert into test_chartab
values( 8864 , '001407000001DD2E' , 228 , '2019-12-14 02:52:05.863');
insert into test_chartab
values( 8890 , '001407000001DD2E' , 228 , '2019-12-20 02:52:05.863');

ALTER TABLE test_chartab SET (timescaledb.compress, timescaledb.compress_segmentby = 'mac_id', timescaledb.compress_orderby = 'ts DESC');

select * from test_chartab order by mac_id , ts limit 2;

--compress the data and check --
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
select * from test_chartab order by mac_id , ts limit 2;

SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
select * from test_chartab order by mac_id , ts limit 2;

-- test constraintawareappend sort node handling
SET enable_hashagg TO false;

CREATE TABLE public.merge_sort (time timestamp NOT NULL, measure_id integer NOT NULL, device_id integer NOT NULL, value float);
SELECT create_hypertable('merge_sort', 'time');
ALTER TABLE merge_sort SET (timescaledb.compress = true, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id, measure_id');

INSERT INTO merge_sort SELECT time, 1, 1, extract(epoch from time) * 0.001 FROM generate_series('2000-01-01'::timestamp,'2000-02-01'::timestamp,'1h'::interval) g1(time);

ANALYZE merge_sort;

--compress first chunk
SELECT
  compress_chunk(c.schema_name || '.' || c.table_name)
FROM _timescaledb_catalog.chunk c
  INNER JOIN _timescaledb_catalog.hypertable ht ON c.hypertable_id=ht.id
WHERE ht.table_name = 'merge_sort'
ORDER BY c.id LIMIT 1;

-- this should have a MergeAppend with children wrapped in Sort nodes
EXPLAIN (analyze,costs off,timing off,summary off) SELECT
  last(time, time) as time,
  device_id,
  measure_id,
  last(value, time) AS value
FROM merge_sort
WHERE time < now()
GROUP BY 2, 3;

-- this should exclude the decompressed chunk
EXPLAIN (analyze,costs off,timing off,summary off) SELECT
  last(time, time) as time,
  device_id,
  measure_id,
  last(value, time) AS value
FROM merge_sort
WHERE time > '2000-01-10'::text::timestamp
GROUP BY 2, 3;

RESET enable_hashagg;

-- test if volatile function quals are applied to compressed chunks
CREATE FUNCTION check_equal_228( intval INTEGER) RETURNS BOOL
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    retval BOOL;
BEGIN
   IF intval = 228 THEN RETURN TRUE;
   ELSE RETURN FALSE;
   END IF;
END;
$BODY$;

-- the function cannot be pushed down to compressed chunk
-- but should be applied as a filter on decompresschunk
SELECT * from test_chartab
WHERE check_equal_228(rtt) ORDER BY ts;

EXPLAIN (analyze,costs off,timing off,summary off)
SELECT * from test_chartab
WHERE check_equal_228(rtt) and ts < '2019-12-15 00:00:00' order by ts;

-- test pseudoconstant qual #3241
CREATE TABLE pseudo(time timestamptz NOT NULL);
SELECT create_hypertable('pseudo','time');
ALTER TABLE pseudo SET (timescaledb.compress);
INSERT INTO pseudo SELECT '2000-01-01';
SELECT compress_chunk(show_chunks('pseudo'));

SELECT * FROM pseudo WHERE now() IS NOT NULL;


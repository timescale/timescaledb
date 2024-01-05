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
SELECT compress_chunk(ch) FROM show_chunks('merge_sort') ch LIMIT 1;

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

-- ensure needed decompression paths underneath a sort node
-- are not recycled by PostgreSQL
SET random_page_cost = 4.0;
DROP TABLE IF EXISTS options_data;

CREATE TABLE IF NOT EXISTS options_data (
    date            date NOT NULL,
    contract_code   text NOT NULL,
    expiry_code     text NOT NULL,
    option_type     character(1) NOT NULL,
    strike          real NOT NULL,
    price           double precision,
    source          text NOT NULL,
    meta            text
);

SELECT create_hypertable('options_data', 'date', chunk_time_interval => interval '1 day');

INSERT INTO options_data (date, contract_code, expiry_code, option_type, strike, price, source, meta)
SELECT
    date_series,
    'CONTRACT' || date_series::text,
    'EXPIRY' || date_series::text,
    CASE WHEN random() < 0.5 THEN 'C' ELSE 'P' END, -- Randomly choose 'C' or 'P' for option_type
   random() * 100, -- Random strike value between 0 and 100
   random() * 100, -- Random price value between 0 and 100
   'SOURCE' || date_series::text,
   'META' || date_series::text
FROM generate_series(
    '2023-12-01 00:00:00',
    '2023-12-05 00:00:00',
    INTERVAL '10 minute'
) AS date_series;

ALTER TABLE options_data SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'contract_code, expiry_code, option_type, strike, source',
    timescaledb.compress_orderby = 'date DESC');

SELECT compress_chunk(ch) FROM show_chunks('options_data', older_than=> '2023-12-05') ch;

ANALYZE options_data;

SELECT max(date) AS date FROM options_data WHERE contract_code='CONTRACT2023-12-03 04:00:00+01';

RESET random_page_cost;

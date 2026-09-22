-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;

CREATE TABLE ht_metrics_compressed(time timestamptz, device int, value float, tag text);
ALTER TABLE ht_metrics_compressed SET (autovacuum_enabled = false);
SELECT create_hypertable('ht_metrics_compressed','time',create_default_indexes:=false, chunk_time_interval => interval '100 day');
ALTER TABLE ht_metrics_compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby='time');
-- helper function: float -> pseudorandom float [0..1].
CREATE OR REPLACE FUNCTION mix(x float4) RETURNS float4 AS $$ SELECT ((hashfloat4(x) / (pow(2., 31) - 1) + 1) / 2)::float4 $$ LANGUAGE SQL;


INSERT INTO ht_metrics_compressed
SELECT
    '2020-05-18'::timestamptz + interval '1 second' * (x + 0.1 * mix(device + x * 10)),
    device,
    100 * mix(device) * sin(x / 3600)
        + 100 * mix(device + 1) * sin(x / (3600 * 24))
        + 100 * mix(device + 2) * sin(x / (3600 * 24 * 7))
        + mix(device + x * 10 + 1),
     format('this-is-a-long-tag-#%s', x % 29)
FROM generate_series(1, 3600 * 24 * 90, 100) x, generate_series(1,2) device;

analyze ht_metrics_compressed;

select show_chunks('ht_metrics_compressed') as "CHUNK" limit 1 \gset
select count(*) from :CHUNK;
select count(distinct tag) from :CHUNK;
select device, min(time), max(time) from :CHUNK group by 1 order by 1, 2, 3;

-- decompress with no indexes
select where compress_chunk(:'CHUNK') is null;
select where decompress_chunk(:'CHUNK') is null;


-- decompress with one index
select where compress_chunk(:'CHUNK') is null;
create index on :CHUNK(device, time);
select where decompress_chunk(:'CHUNK') is null;


-- decompress with two indexes
select where compress_chunk(:'CHUNK') is null;
create index on :CHUNK(tag);
select where decompress_chunk(:'CHUNK') is null;


-- check the data after decompression
set enable_seqscan to off;
select count(*) from :CHUNK;
select count(distinct tag) from :CHUNK;
select distinct on (device) device, time from :CHUNK order by 1, 2;

-- check that the indexes are used
explain (buffers off, costs off) select count(distinct tag) from :CHUNK;
explain (buffers off, costs off) select distinct on (device) device, time from :CHUNK order by 1, 2;

RESET timescaledb.enable_skipscan_for_distinct_aggregates;
drop table ht_metrics_compressed;

-- Fix for issue #8681: IndexScan is not chosen for columnstore segmented on varchar column
-- We should chose IndexScan now, and use SkipScan as well
CREATE TABLE record (time timestamptz not null, data varchar);
SELECT table_name FROM create_hypertable('record','time');
ALTER TABLE record SET (timescaledb.compress,timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='data');

INSERT INTO record
SELECT time, (array['Yes', 'No', 'Maybe'])[floor(random() * 3 + 1)]
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03'::timestamptz, '10 minute'::interval) AS g1(time);

analyze record;
SELECT compress_chunk(ch) FROM show_chunks('record') ch;

-- enable_seqscan is OFF, should see IndexScan
explain (buffers off, costs off) SELECT * FROM record ORDER BY data;
-- SkipScan is chosen because IndexScan is chosen
explain (buffers off, costs off) SELECT DISTINCT ON(data) * FROM record;
-- (seg_col = const) condition is checked even when seg_col is coerced
explain (buffers off, costs off) SELECT * FROM record WHERE data='Yes' ORDER BY data;
explain (buffers off, costs off) SELECT * FROM record WHERE 'Yes' <= data ORDER BY data;

drop table record cascade;

-- Fix for issue #9066: IndexScan is not chosen for columnstore segmented on several keys
-- for a query sorted on columnstore keys
-- but where one numeric key is pinned to a Const of different but compatible type.
-- We should chose IndexScan now, and use SkipScan as well.
CREATE TABLE log_numeric(
	"time"       timestamp with time zone NOT NULL,
	device_id    integer                  NOT NULL,
	parameter_id smallint                 NOT NULL,
	value        double precision
);

SELECT create_hypertable('log_numeric', 'time', chunk_time_interval => interval '1 days', create_default_indexes => false);

ALTER TABLE log_numeric SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id, parameter_id', timescaledb.compress_orderby='"time" DESC');

INSERT INTO log_numeric
SELECT time, device_id, parameter_id, device_id*parameter_id
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03'::timestamptz, '10 minute'::interval) AS g1(time),
generate_series(1,4) device_id, generate_series(1,4) parameter_id;

select compress_chunk(ch) from show_chunks('log_numeric') ch;

-- enable_seqscan is OFF, should see IndexScan
explain (buffers off, costs off) SELECT * FROM log_numeric WHERE parameter_id = 1 ORDER BY device_id, parameter_id, time DESC;
-- SkipScan is chosen because IndexScan is chosen
explain (buffers off, costs off) SELECT DISTINCT ON(device_id, parameter_id) * FROM log_numeric WHERE parameter_id = 1 ORDER BY device_id, parameter_id, time DESC;

drop table log_numeric cascade;



-- Join condition on a segmentby column is pushed down to the compressed index
-- when the column is explicitly cast.
CREATE TABLE reading(time timestamptz not null, device varchar not null, value float);
SELECT table_name FROM create_hypertable('reading','time',chunk_time_interval => interval '1 day');
ALTER TABLE reading SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby='time desc');

INSERT INTO reading
SELECT time, 'dev' || device, random()
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03'::timestamptz, '10 minute'::interval) AS g1(time),
generate_series(1,10) device;

SELECT count(compress_chunk(ch)) FROM show_chunks('reading') ch;

CREATE TABLE device(name varchar not null primary key);
INSERT INTO device VALUES ('dev3');
ANALYZE reading, device;

explain (buffers off, costs off) SELECT count(*) FROM reading r JOIN device d ON r.device::text = d.name::text;
explain (buffers off, costs off) SELECT count(*) FROM reading r JOIN device d ON r.device = d.name;

drop table reading cascade;
drop table device;

-- A coercion that is not a relabel cannot be pushed down, the join condition
-- is checked after decompression.
CREATE TABLE reading_num(time timestamptz not null, device int not null, value float);
SELECT table_name FROM create_hypertable('reading_num','time',chunk_time_interval => interval '1 day');
ALTER TABLE reading_num SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby='time desc');

INSERT INTO reading_num
SELECT time, device, random()
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03'::timestamptz, '10 minute'::interval) AS g1(time),
generate_series(1,10) device;

SELECT count(compress_chunk(ch)) FROM show_chunks('reading_num') ch;

CREATE TABLE device_num(val numeric not null primary key);
INSERT INTO device_num VALUES (3);
ANALYZE reading_num, device_num;

explain (buffers off, costs off) SELECT count(*) FROM reading_num r JOIN device_num d ON r.device = d.val;
SELECT count(*) FROM reading_num r JOIN device_num d ON r.device = d.val;
explain (buffers off, costs off) SELECT count(*) FROM reading_num r JOIN device_num d ON r.device::numeric = d.val;
SELECT count(*) FROM reading_num r JOIN device_num d ON r.device::numeric = d.val;

drop table reading_num cascade;
drop table device_num;

-- Two segmentby columns where only the first one can be pushed down. The
-- condition on the second one is checked on the decompressed tuple.
CREATE TABLE reading_mixed(time timestamptz not null, ident varchar not null, device int not null, value float);
SELECT table_name FROM create_hypertable('reading_mixed','time',chunk_time_interval => interval '1 day');
ALTER TABLE reading_mixed SET (timescaledb.compress, timescaledb.compress_segmentby='ident,device', timescaledb.compress_orderby='time desc');

INSERT INTO reading_mixed
SELECT time, 'dev' || device, device, random()
FROM generate_series('2000-01-01'::timestamptz,'2000-01-03'::timestamptz, '10 minute'::interval) AS g1(time),
generate_series(1,10) device;

SELECT count(compress_chunk(ch)) FROM show_chunks('reading_mixed') ch;

CREATE TABLE device_mixed(ident varchar not null, val numeric not null);
INSERT INTO device_mixed VALUES ('dev3', 3);
ANALYZE reading_mixed, device_mixed;

explain (buffers off, costs off) SELECT count(*) FROM reading_mixed r JOIN device_mixed d ON r.ident = d.ident AND r.device = d.val;
SELECT count(*) FROM reading_mixed r JOIN device_mixed d ON r.ident = d.ident AND r.device = d.val;

drop table reading_mixed cascade;
drop table device_mixed;

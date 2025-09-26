-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

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

-- To avoid differences in PG15 output which doesn't support this feature
SET timescaledb.enable_skipscan_for_distinct_aggregates TO false;

-- check that the indexes are used
explain (costs off) select count(distinct tag) from :CHUNK;
explain (costs off) select distinct on (device) device, time from :CHUNK order by 1, 2;

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

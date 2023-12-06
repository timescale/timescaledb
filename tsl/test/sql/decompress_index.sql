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


-- check that the indexes are used
explain (costs off) select count(distinct tag) from :CHUNK;
explain (costs off) select distinct on (device) device, time from :CHUNK order by 1, 2;


drop table ht_metrics_compressed;

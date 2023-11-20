-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

CREATE TABLE ht_metrics_compressed(time timestamptz, device int, value float, tag text);
ALTER TABLE ht_metrics_compressed SET (autovacuum_enabled = false);
SELECT create_hypertable('ht_metrics_compressed','time',create_default_indexes:=false);
ALTER TABLE ht_metrics_compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby='time');
-- helper function: float -> pseudorandom float [0..1].
CREATE OR REPLACE FUNCTION mix(x float4) RETURNS float4 AS $$ SELECT ((hashfloat4(x) / (pow(2., 31) - 1) + 1) / 2)::float4 $$ LANGUAGE SQL;


INSERT INTO ht_metrics_compressed
SELECT
    '2020-01-08'::timestamptz + interval '1 second' * (x + 0.1 * mix(device + x * 10)),
    device,
    100 * mix(device) * sin(x / 3600)
        + 100 * mix(device + 1) * sin(x / (3600 * 24))
        + 100 * mix(device + 2) * sin(x / (3600 * 24 * 7))
        + mix(device + x * 10 + 1),
     format('this-is-a-long-tag-#%s', x % 29)
FROM generate_series(1, 3600 * 24 * 356, 100) x, generate_series(1,2) device;

-- compress it all
SELECT count(compress_chunk(c, true)) FROM show_chunks('ht_metrics_compressed') c;

select count(*) from ht_metrics_compressed;

-- Helper function that returns the amount of memory currently allocated in a
-- given memory context.
create or replace function ts_debug_allocated_bytes(text = 'PortalContext') returns bigint
    as :MODULE_PATHNAME, 'ts_debug_allocated_bytes'
    language c strict volatile;

-- Check that decompression doesn't leak memory. Record memory usage after each
-- compressed chunk, and use linear regression to tell if memory usage grows.
with log as materialized (
        select rank() over (order by c) n, ts_debug_allocated_bytes() b, decompress_chunk(c, true)
        from show_chunks('ht_metrics_compressed') c order by c)
    , regression as (select regr_slope(b, n) slope, regr_intercept(b, n) intercept from log)
select * from log
    where (select slope / intercept::float > 0.01 from regression)
;

-- Same as above but for compression.
with log as materialized (
        select rank() over (order by c) n, ts_debug_allocated_bytes() b, compress_chunk(c, true)
        from show_chunks('ht_metrics_compressed') c order by c)
    , regression as (select regr_slope(b, n) slope, regr_intercept(b, n) intercept from log)
select * from log
    where (select slope / intercept::float > 0.01 from regression)
;

-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

SELECT 'NULL::'||:'TYPE' as "NULLTYPE" \gset

--compress the data
SELECT count(compress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like :'HYPERTABLE_NAME' and chunk.status & 1 = 0;

SELECT
    comp_hypertable.schema_name AS "COMP_SCHEMA_NAME",
    comp_hypertable.table_name AS "COMP_TABLE_NAME"
FROM _timescaledb_catalog.hypertable uc_hypertable
INNER JOIN _timescaledb_catalog.hypertable comp_hypertable ON (comp_hypertable.id = uc_hypertable.compressed_hypertable_id)
WHERE uc_hypertable.table_name like :'HYPERTABLE_NAME' \gset

SELECT
     bool_and(:SEGMENT_META_COL_MIN = true_min) as min_correct,
     bool_and(:SEGMENT_META_COL_MAX = true_max) as max_correct
FROM
:"COMP_SCHEMA_NAME".:"COMP_TABLE_NAME", LATERAL (
    SELECT min(decomp) true_min, max(decomp) true_max, ((count(*)-count(decomp)) > 0) true_has_null
    FROM _timescaledb_internal.decompress_forward(:"ORDER_BY_COL_NAME", :NULLTYPE) decomp
    )
as m;

\set ECHO all

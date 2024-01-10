-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

SELECT 'NULL::'||:'TYPE' as "NULLTYPE" \gset

--compress the data
SELECT count(compress_chunk(ch, true)) FROM show_chunks(:'HYPERTABLE_NAME') ch;

SELECT
    ch.schema_name AS "COMP_SCHEMA_NAME",
    ch.table_name AS "COMP_TABLE_NAME"
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.chunk ch ON (ch.hypertable_id = ht.compressed_hypertable_id)
WHERE ht.table_name like :'HYPERTABLE_NAME' ORDER BY ch.id LIMIT 1\gset

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

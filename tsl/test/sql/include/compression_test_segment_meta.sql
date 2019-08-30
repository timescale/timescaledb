-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

SELECT 'NULL::'||:'TYPE' as "NULLTYPE" \gset

SELECT
     _timescaledb_internal.segment_meta_min_max_agg(i)::text as "META_TEXT",
     min(i) as "TRUE_MIN",
     max(i) as "TRUE_MAX",
     (count(*)-count(i)) > 0 as "TRUE_HAS_NULL"
FROM :"TABLE" \gset

SELECT
    _timescaledb_internal.segment_meta_get_min(meta, :NULLTYPE) = :'TRUE_MIN' as min_correct,
    _timescaledb_internal.segment_meta_get_max(meta, :NULLTYPE) = :'TRUE_MAX' as max_correct,
    _timescaledb_internal.segment_meta_has_null(meta) = :'TRUE_HAS_NULL' as has_null_correct
FROM
(
        SELECT
            :'META_TEXT'::_timescaledb_internal.segment_meta_min_max as meta
) AS meta_gen;



\set ECHO all

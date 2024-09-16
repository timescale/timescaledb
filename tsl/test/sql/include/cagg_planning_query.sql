-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SHOW timescaledb.enable_cagg_sort_pushdown;

:PREFIX SELECT 'query 01' AS label, * FROM cagg1 ORDER BY time_bucket;
:PREFIX SELECT 'query 02' AS label, * FROM cagg1 ORDER BY time_bucket DESC;

:PREFIX SELECT 'query 03' AS label, * FROM cagg1_ordered_asc ORDER BY time_bucket;
:PREFIX SELECT 'query 04' AS label, * FROM cagg1_ordered_asc ORDER BY time_bucket DESC;

:PREFIX SELECT 'query 05' AS label, * FROM cagg1_ordered_desc ORDER BY time_bucket;
:PREFIX SELECT 'query 06' AS label, * FROM cagg1_ordered_desc ORDER BY time_bucket DESC;

:PREFIX SELECT 'query 07' AS label, * FROM cagg2 ORDER BY time_bucket;
:PREFIX SELECT 'query 08' AS label, * FROM cagg2 ORDER BY time_bucket DESC;

:PREFIX SELECT 'query 07' AS label, * FROM cagg3 ORDER BY time_bucket;
:PREFIX SELECT 'query 08' AS label, * FROM cagg3 ORDER BY time_bucket DESC;

-- not optimized atm
:PREFIX SELECT 'query 101' AS label, * FROM cagg2 ORDER BY time_bucket::date;
:PREFIX SELECT 'query 102' AS label, * FROM cagg2 ORDER BY time_bucket::date DESC;


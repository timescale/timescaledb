-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


-- Check that in chunkwise grouping, the cardinality for individual chunks is
-- estimated properly, by comparing it to the direct chunk query.
explain (buffers off)
select count(*) from metrics group by v0;

select chunk from show_chunks('metrics') chunk order by chunk limit 1 \gset

explain (buffers off)
select count(*) from :chunk group by v0;

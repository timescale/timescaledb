-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test the case where the chunk is present both as a separate table and as a
-- child of a hypertable. #4708

select show_chunks('metrics_compressed') chunk order by 1 limit 1 \gset
select * from metrics_compressed inner join :chunk on (false);

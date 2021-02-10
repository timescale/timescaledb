-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Incremental sort feature is available in PG13 and beyond

-- Disable incremental sort. The query will use sequential scans
SET enable_incremental_sort = OFF;

\o :UNOPTIMIZED_OUTPUT
:PREFIX
SELECT * FROM :TEST_TABLE ORDER BY device, time LIMIT 51;

-- Enable incremental sort. The query should use it now
SET enable_incremental_sort = ON;

\o :INCR_SORT_OUTPUT
:PREFIX
SELECT * FROM :TEST_TABLE ORDER BY device, time LIMIT 51;

-- Use the first column in the query in ORDER BY and also use LIMIT
-- This should use an "ordered append" plan along with incremental sort

\o :ORDERED_OUTPUT
:PREFIX
SELECT * FROM :TEST_TABLE ORDER BY time, device LIMIT 51;

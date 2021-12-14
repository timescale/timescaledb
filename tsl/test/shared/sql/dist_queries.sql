-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test DataNodeScan with subquery with one-time filter
SELECT
  id
FROM
  insert_test
WHERE
  NULL::int2 >= NULL::int2 OR
  EXISTS (SELECT 1 from dist_chunk_copy WHERE insert_test.id IS NOT NULL)
ORDER BY id;


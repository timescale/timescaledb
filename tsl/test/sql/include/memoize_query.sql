-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SHOW enable_memoize;

:PREFIX
SELECT m1.time, m2.time
FROM :TEST_TABLE m1
LEFT JOIN LATERAL (
    SELECT time FROM :TEST_TABLE m2
    WHERE m1.device_id = m2.device_id
    ORDER BY m2.time DESC
    LIMIT 1
) m2 ON true
ORDER BY m1.time
LIMIT 10000
;

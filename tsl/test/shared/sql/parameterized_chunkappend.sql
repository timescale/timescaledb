-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for parameterized ChunkAppend.

\set PREFIX 'EXPLAIN (COSTS OFF, ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY OFF)'

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;

:PREFIX
WITH win(id, win_start, win_end) AS (VALUES
    (1, '2000-01-03 00:00'::timestamptz, '2000-01-03 12:00'::timestamptz),
    (2, '2000-01-08 00:00', '2000-01-08 12:00'),
    (3, '2000-01-15 00:00', '2000-01-15 12:00'))
SELECT devices.device_id, win.id, count(*)
FROM devices, win,
LATERAL (
    SELECT * FROM metrics
    WHERE metrics.device_id = devices.device_id
      AND metrics.time >= win.win_start
      AND metrics.time < win.win_end
      AND metrics.time > now() - interval '100 years'
) sub
GROUP BY devices.device_id, win.id
ORDER BY devices.device_id, win.id
;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;

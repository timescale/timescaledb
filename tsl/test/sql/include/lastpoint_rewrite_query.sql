-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT DISTINCT ON (device) * FROM lastpoint ORDER BY device, time DESC, tiebreak DESC;
SELECT DISTINCT ON (device) device, time, v FROM lastpoint WHERE time < '2025-01-03' ORDER BY device, time DESC, tiebreak;
SELECT DISTINCT ON (device) time, v * 2 AS v2 FROM lastpoint ORDER BY device, time DESC, tiebreak LIMIT 5 OFFSET 2;
SELECT DISTINCT ON (device) * FROM lastpoint ORDER BY device DESC, time DESC, tiebreak;
SELECT DISTINCT ON (device) * FROM lastpoint WHERE v > 50 ORDER BY device, time, tiebreak;
SELECT DISTINCT ON (device) device, time FROM lastpoint WHERE device > 'd15' ORDER BY device, time DESC;
SELECT DISTINCT ON (nullable_device) nullable_device, time, v FROM lastpoint ORDER BY nullable_device, time DESC, tiebreak;
SELECT DISTINCT ON (nullable_device) nullable_device, time, v FROM lastpoint ORDER BY nullable_device NULLS FIRST, time DESC, tiebreak;

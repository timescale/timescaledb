-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Full contents of the table under test, ordered so the dump is stable.
-- Captured before and after a move and compared with diff.
SELECT * FROM metrics ORDER BY time, device;

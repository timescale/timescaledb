-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test binary compatibility. The hash values might change if the UMASH custom
-- hashing is not built on this platform, we had this problem before.
select _timescaledb_functions.bloom1_contains(
	'\xd098c885f08468eb8916751d947f248ed2843a88c02b1dea6228591c588b8068'::_timescaledb_internal.bloom1,
	'1y.yield'::text)
;


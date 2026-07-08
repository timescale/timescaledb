-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Debug-only diagnostic (see ts_tenant_tracker_info in
-- tenant_tracker_function.c): current per-tenant tracker state for a hypertable
-- -- seqnum, active generation, occupied entries, status (0 = valid, 1 =
-- invalid), and the epoch late-arrival window [late_threshold_start,
-- late_threshold_end) in internal time -- or all-NULL when the hypertable has no
-- tracker.  Shared-memory tracker state is not otherwise SQL-inspectable.
-- FIXME: promote to a permanent extension-wide function (see the C file).
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_tenant_tracker(
    hypertable regclass,
    OUT seq_num int4,
    OUT active_generation int4,
    OUT nentries int4,
    OUT status int4,
    OUT late_threshold_start int8,
    OUT late_threshold_end int8)
AS :TSL_MODULE_PATHNAME, 'ts_tenant_tracker_info' LANGUAGE C VOLATILE;

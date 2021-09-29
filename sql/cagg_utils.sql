-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Adds the initial materialization invalidation log entry to all data nodes that comprise the
-- underlying distributed hypertable when creating a CAGG. This invalidation entry and is set
-- to [-infinity, +infinity].
--
-- mat_hypertable_id - The hypertable ID of the CAGG materialized hypertable in the Access Node
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_cagg_log_add_initial_entry(
    mat_hypertable_id INTEGER
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_invalidation_cagg_log_add_initial_entry' LANGUAGE C STRICT VOLATILE;

-- Processes the hypertable invalidation log in a data node for all the CAGGs that belong to the
-- distributed hypertable with hypertable ID 'raw_hypertable_id' in the Access Node. The
-- invalidations are cut, merged and moved to the materialization invalidation log.
--
-- mat_hypertable_id - The hypertable ID of the CAGG materialized hypertable in the Access Node
--                     that is currently being refreshed
-- raw_hypertable_id - The hypertable ID of the original distributed hypertable in the Access Node
-- dimtype - The OID of the type of the time dimension for this CAGG
-- last_mat_hypertable_id - The hypertable ID of the last CAGG materialized hypertable in the
--                          Access Node from the 'mat_hypertable_ids' array
-- mat_hypertable_ids - The array of hypertable IDs for all CAGG materialized hypertables in the
--                      Access Node that belong to 'raw_hypertable_id'
-- bucket_widths - The array of time bucket widths for all the CAGGs that belong to
--                 'raw_hypertable_id'
-- max_bucket_widths - The array of the maximum time bucket widths for all the CAGGs that belong
--                     to 'raw_hypertable_id'
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_process_hypertable_log(
    mat_hypertable_id INTEGER,
    raw_hypertable_id INTEGER,
    dimtype REGCLASS,
    last_mat_hypertable_id INTEGER,
    mat_hypertable_ids INTEGER[],
    bucket_widths BIGINT[],
    max_bucket_widths BIGINT[]
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_invalidation_process_hypertable_log' LANGUAGE C STRICT VOLATILE;

-- Processes the materialization invalidation log in a data node for the CAGG being refreshed that
-- belongs to the distributed hypertable with hypertable ID 'raw_hypertable_id' in the Access Node.
-- The invalidations are cut, merged and returned as a single refresh window.
--
-- mat_hypertable_id - The hypertable ID of the CAGG materialized hypertable in the Access Node
--                     that is currently being refreshed.
-- raw_hypertable_id - The hypertable ID of the original distributed hypertable in the Access Node
-- dimtype - The OID of the type of the time dimension for this CAGG
-- window_start - The starting time of the CAGG refresh window
-- window_end - The ending time of the CAGG refresh window
-- last_mat_hypertable_id - The hypertable ID of the last CAGG materialized hypertable in the
--                          Access Node from the 'mat_hypertable_ids' array
-- mat_hypertable_ids - The array of hypertable IDs for all CAGG materialized hypertables in the
--                      Access Node that belong to 'raw_hypertable_id'
-- bucket_widths - The array of time bucket widths for all the CAGGs that belong to
--                 'raw_hypertable_id'
-- max_bucket_widths - The array of the maximum time bucket widths for all the CAGGs that belong
--                     to 'raw_hypertable_id'
--
-- Returns a tuple of:
-- ret_window_start - The merged refresh window starting time
-- ret_window_end - The merged refresh window ending time
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_process_cagg_log(
    mat_hypertable_id INTEGER,
    raw_hypertable_id INTEGER,
    dimtype REGCLASS,
    window_start BIGINT,
    window_end BIGINT,
    last_mat_hypertable_id INTEGER,
    mat_hypertable_ids INTEGER[],
    bucket_widths BIGINT[],
    max_bucket_widths BIGINT[],
    OUT ret_window_start BIGINT,
    OUT ret_window_end BIGINT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_invalidation_process_cagg_log' LANGUAGE C STRICT VOLATILE;

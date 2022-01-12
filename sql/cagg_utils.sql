-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Adds a materialization invalidation log entry to the local data node
--
-- mat_hypertable_id - The hypertable ID of the CAGG materialized hypertable in the Access Node
-- start_time - The starting time of the materialization invalidation log entry
-- end_time - The ending time of the materialization invalidation log entry
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_cagg_log_add_entry(
    mat_hypertable_id INTEGER,
    start_time BIGINT,
    end_time BIGINT
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_invalidation_cagg_log_add_entry' LANGUAGE C STRICT VOLATILE;

-- Adds a materialization invalidation log entry to the local data node
--
-- raw_hypertable_id - The hypertable ID of the original distributed hypertable in the Access Node
-- start_time - The starting time of the materialization invalidation log entry
-- end_time - The ending time of the materialization invalidation log entry
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_hyper_log_add_entry(
    raw_hypertable_id INTEGER,
    start_time BIGINT,
    end_time BIGINT
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_invalidation_hyper_log_add_entry' LANGUAGE C STRICT VOLATILE;

-- raw_hypertable_id - The hypertable ID of the original distributed hypertable in the Access Node
CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_invalidation_log_delete(
    raw_hypertable_id INTEGER
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_hypertable_invalidation_log_delete' LANGUAGE C STRICT VOLATILE;

-- mat_hypertable_id - The hypertable ID of the CAGG materialized hypertable in the Access Node
CREATE OR REPLACE FUNCTION _timescaledb_internal.materialization_invalidation_log_delete(
    mat_hypertable_id INTEGER
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_materialization_invalidation_log_delete' LANGUAGE C STRICT VOLATILE;

-- raw_hypertable_id - The hypertable ID of the original distributed hypertable in the Access Node
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_dist_ht_invalidation_trigger(
    raw_hypertable_id INTEGER
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_drop_dist_ht_invalidation_trigger' LANGUAGE C STRICT VOLATILE;

-- Processes the hypertable invalidation log in a data node for all the CAGGs that belong to the
-- distributed hypertable with hypertable ID 'raw_hypertable_id' in the Access Node. The
-- invalidations are cut, merged and moved to the materialization invalidation log.
--
-- mat_hypertable_id - The hypertable ID of the CAGG materialized hypertable in the Access Node
--                     that is currently being refreshed
-- raw_hypertable_id - The hypertable ID of the original distributed hypertable in the Access Node
-- dimtype - The OID of the type of the time dimension for this CAGG
-- mat_hypertable_ids - The array of hypertable IDs for all CAGG materialized hypertables in the
--                      Access Node that belong to 'raw_hypertable_id'
-- bucket_widths - The array of time bucket widths for all the CAGGs that belong to
--                 'raw_hypertable_id'
-- max_bucket_widths - (Deprecated) This argument is ignored and is present only
--                     for backward compatibility.
-- bucket_functions - (Optional) The array of serialized information about bucket functions
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_process_hypertable_log(
    mat_hypertable_id INTEGER,
    raw_hypertable_id INTEGER,
    dimtype REGTYPE,
    mat_hypertable_ids INTEGER[],
    bucket_widths BIGINT[],
    max_bucket_widths BIGINT[]
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_invalidation_process_hypertable_log' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_process_hypertable_log(
    mat_hypertable_id INTEGER,
    raw_hypertable_id INTEGER,
    dimtype REGTYPE,
    mat_hypertable_ids INTEGER[],
    bucket_widths BIGINT[],
    max_bucket_widths BIGINT[],
    bucket_functions TEXT[]
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
-- mat_hypertable_ids - The array of hypertable IDs for all CAGG materialized hypertables in the
--                      Access Node that belong to 'raw_hypertable_id'
-- bucket_widths - The array of time bucket widths for all the CAGGs that belong to
--                 'raw_hypertable_id'
-- max_bucket_widths - (Deprecated) This argument is ignored and is present only
--                     for backward compatibility.
-- bucket_functions - (Optional) The array of serialized information about bucket functions
--
-- Returns a tuple of:
-- ret_window_start - The merged refresh window starting time
-- ret_window_end - The merged refresh window ending time
CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_process_cagg_log(
    mat_hypertable_id INTEGER,
    raw_hypertable_id INTEGER,
    dimtype REGTYPE,
    window_start BIGINT,
    window_end BIGINT,
    mat_hypertable_ids INTEGER[],
    bucket_widths BIGINT[],
    max_bucket_widths BIGINT[],
    OUT ret_window_start BIGINT,
    OUT ret_window_end BIGINT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_invalidation_process_cagg_log' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.invalidation_process_cagg_log(
    mat_hypertable_id INTEGER,
    raw_hypertable_id INTEGER,
    dimtype REGTYPE,
    window_start BIGINT,
    window_end BIGINT,
    mat_hypertable_ids INTEGER[],
    bucket_widths BIGINT[],
    max_bucket_widths BIGINT[],
    bucket_functions TEXT[],
    OUT ret_window_start BIGINT,
    OUT ret_window_end BIGINT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_invalidation_process_cagg_log' LANGUAGE C STRICT VOLATILE;

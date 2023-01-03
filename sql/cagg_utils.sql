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

--Get the maximum value materialized for the given hypertable
CREATE OR REPLACE FUNCTION timescaledb_experimental.get_materialization_threshold(
   mat_ht varchar,
   time_col varchar
)  RETURNS varchar
LANGUAGE 'plpgsql'
 AS    $func$
 DECLARE mat_thresh TIMESTAMP;
         query_string text;
BEGIN
    query_string = format('SELECT max(%s) FROM %s', time_col, mat_ht);
    EXECUTE query_string into mat_thresh;
    RETURN mat_thresh;
END;
$func$;

--Function to materialize the hypertable --mat_ht using join definition in join_view
--using the target hypertable target_ht over the provided target_thresh
CREATE OR REPLACE FUNCTION timescaledb_experimental.execute_materialization(
    join_view varchar,
    mat_ht varchar,
    target_ht varchar,
    time_col varchar,
    prev_mat_thresh varchar,
    target_mat_thresh varchar
)  RETURNS VOID
 LANGUAGE 'plpgsql'
 AS    $func$
BEGIN
   EXECUTE format('LOCK TABLE ONLY %s IN SHARE ROW EXCLUSIVE MODE', target_ht);
   EXECUTE format('REFRESH MATERIALIZED VIEW %s',join_view);
   EXECUTE format('INSERT INTO %s SELECT * FROM %s WHERE %s > ''%s'' AND %s <= ''%s''', mat_ht, join_view, time_col, prev_mat_thresh, time_col, target_mat_thresh);
END
 $func$;

/*
 * Set a new invalidation threshold.
 *
 * The threshold is only updated if the new threshold is greater than the old
 * one.
 *
 * On success, the new threshold is returned, otherwise the existing threshold
 * is returned instead.
 */
CREATE OR REPLACE FUNCTION timescaledb_experimental.get_set_inval_threshold(
    target_ht REGCLASS,
    target_mat_thresh TIMESTAMP
)  RETURNS TIMESTAMP
LANGUAGE 'plpgsql'
 AS    $func$
 DECLARE mat_thresh TIMESTAMP;
        query_string text;
        htid Oid;
        ht_name varchar;
BEGIN
   query_string = format('select relname from pg_class where oid = %d', target_ht);
   EXECUTE query_string into ht_name;
   query_string = format('SELECT id from _timescaledb_catalog.hypertable where table_name ='%s'',ht_name);
   EXECUTE query_string into htid;
   query_string = format('SELECT watermark from _timescaledb_catalog.continuous_aggs_invalidation_threshold where hypertable_id = %d',htid);
   EXECUTE query_string into mat_thresh
   if target_mat_thresh >mat_thresh then
        EXECUTE format('Update _timescaledb_catalog.continuous_aggs_invalidation_threshold set watermark = %d where hypertable_id = %d', target_mat_thresh, target_ht);
        end if;
   RETURN target_mat_thresh
END
 $func$;

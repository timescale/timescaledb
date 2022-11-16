-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Convenience view to list all hypertables
CREATE OR REPLACE VIEW timescaledb_information.hypertables AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  t.tableowner AS owner,
  ht.num_dimensions,
  (
    SELECT count(1)
    FROM _timescaledb_catalog.chunk ch
    WHERE ch.hypertable_id = ht.id) AS num_chunks,
  (
    CASE WHEN compression_state = 1 THEN
      TRUE
    ELSE
      FALSE
    END) AS compression_enabled,
  (
    CASE WHEN ht.replication_factor > 0 THEN
      TRUE
    ELSE
      FALSE
    END) AS is_distributed,
  ht.replication_factor,
  dn.node_list AS data_nodes,
  srchtbs.tablespace_list AS tablespaces
FROM _timescaledb_catalog.hypertable ht
  INNER JOIN pg_tables t ON ht.table_name = t.tablename
    AND ht.schema_name = t.schemaname
  LEFT OUTER JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = ht.id
  LEFT OUTER JOIN (
    SELECT hypertable_id,
      array_agg(tablespace_name ORDER BY id) AS tablespace_list
    FROM _timescaledb_catalog.tablespace
    GROUP BY hypertable_id) srchtbs ON ht.id = srchtbs.hypertable_id
  LEFT OUTER JOIN (
  SELECT hypertable_id,
    array_agg(node_name ORDER BY node_name) AS node_list
  FROM _timescaledb_catalog.hypertable_data_node
  GROUP BY hypertable_id) dn ON ht.id = dn.hypertable_id
WHERE ht.compression_state != 2 --> no internal compression tables
  AND ca.mat_hypertable_id IS NULL;

CREATE OR REPLACE VIEW timescaledb_information.job_stats AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  j.id AS job_id,
  js.last_start AS last_run_started_at,
  js.last_successful_finish AS last_successful_finish,
  CASE WHEN js.last_finish < '4714-11-24 00:00:00+00 BC' THEN
    NULL
  WHEN js.last_finish IS NOT NULL THEN
    CASE WHEN js.last_run_success = 't' THEN
      'Success'
    WHEN js.last_run_success = 'f' THEN
      'Failed'
    END
  END AS last_run_status,
  CASE WHEN pgs.state = 'active' THEN
    'Running'
  WHEN j.scheduled = FALSE THEN
    'Paused'
  ELSE
    'Scheduled'
  END AS job_status,
  CASE WHEN js.last_finish > js.last_start THEN
  (js.last_finish - js.last_start)
  END AS last_run_duration,
  CASE WHEN j.scheduled THEN
    js.next_start
  END AS next_start,
  js.total_runs,
  js.total_successes,
  js.total_failures
FROM _timescaledb_config.bgw_job j
  INNER JOIN _timescaledb_internal.bgw_job_stat js ON j.id = js.job_id
  LEFT JOIN _timescaledb_catalog.hypertable ht ON j.hypertable_id = ht.id
  LEFT JOIN pg_stat_activity pgs ON pgs.datname = current_database()
    AND pgs.application_name = j.application_name
  ORDER BY ht.schema_name,
    ht.table_name;

-- view for background worker jobs
CREATE OR REPLACE VIEW timescaledb_information.jobs AS
SELECT j.id AS job_id,
  j.application_name,
  j.schedule_interval,
  j.max_runtime,
  j.max_retries,
  j.retry_period,
  j.proc_schema,
  j.proc_name,
  j.owner,
  j.scheduled,
  j.fixed_schedule,
  j.config,
  js.next_start,
  j.initial_start,
  ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  j.check_schema,
  j.check_name
FROM _timescaledb_config.bgw_job j
  LEFT JOIN _timescaledb_catalog.hypertable ht ON ht.id = j.hypertable_id
  LEFT JOIN _timescaledb_internal.bgw_job_stat js ON js.job_id = j.id;

-- views for continuous aggregate queries ---
CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregates AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  cagg.user_view_schema AS view_schema,
  cagg.user_view_name AS view_name,
  viewinfo.viewowner AS view_owner,
  cagg.materialized_only,
  CASE WHEN mat_ht.compressed_hypertable_id IS NOT NULL
       THEN TRUE
       ELSE FALSE
  END AS compression_enabled,
  mat_ht.schema_name AS materialization_hypertable_schema,
  mat_ht.table_name AS materialization_hypertable_name,
  directview.viewdefinition AS view_definition,
  cagg.finalized
FROM _timescaledb_catalog.continuous_agg cagg,
  _timescaledb_catalog.hypertable ht,
  LATERAL (
    SELECT C.oid,
      pg_get_userbyid(C.relowner) AS viewowner
    FROM pg_class C
      LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'v'
      AND C.relname = cagg.user_view_name
      AND N.nspname = cagg.user_view_schema) viewinfo,
  LATERAL (
    SELECT pg_get_viewdef(C.oid) AS viewdefinition
    FROM pg_class C
    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  WHERE C.relkind = 'v'
    AND C.relname = cagg.direct_view_name
    AND N.nspname = cagg.direct_view_schema) directview,
  LATERAL (
    SELECT schema_name, table_name, compressed_hypertable_id
    FROM _timescaledb_catalog.hypertable
    WHERE cagg.mat_hypertable_id = id) mat_ht
WHERE cagg.raw_hypertable_id = ht.id;

CREATE OR REPLACE VIEW timescaledb_information.data_nodes AS
SELECT s.node_name,
  s.owner,
  s.options
FROM (
  SELECT srvname AS node_name,
    srvowner::regrole::name AS owner,
    srvoptions AS options
  FROM pg_catalog.pg_foreign_server AS srv,
    pg_catalog.pg_foreign_data_wrapper AS fdw
  WHERE srv.srvfdw = fdw.oid
    AND fdw.fdwname = 'timescaledb_fdw') AS s;

-- chunks metadata view, shows information about the primary dimension column
-- query plans with CTEs are not always optimized by PG. So use in-line
-- tables.

CREATE OR REPLACE VIEW timescaledb_information.chunks AS
SELECT hypertable_schema,
  hypertable_name,
  schema_name AS chunk_schema,
  chunk_name,
  primary_dimension,
  primary_dimension_type,
  range_start,
  range_end,
  integer_range_start AS range_start_integer,
  integer_range_end AS range_end_integer,
  is_compressed,
  chunk_table_space AS chunk_tablespace,
  node_list AS data_nodes
FROM (
  SELECT ht.schema_name AS hypertable_schema,
    ht.table_name AS hypertable_name,
    srcch.schema_name AS schema_name,
    srcch.table_name AS chunk_name,
    dim.column_name AS primary_dimension,
    dim.column_type AS primary_dimension_type,
    row_number() OVER (PARTITION BY chcons.chunk_id ORDER BY dim.id) AS chunk_dimension_num,
    CASE WHEN (dim.column_type = 'TIMESTAMP'::regtype
      OR dim.column_type = 'TIMESTAMPTZ'::regtype
      OR dim.column_type = 'DATE'::regtype) THEN
      _timescaledb_internal.to_timestamp(dimsl.range_start)
    ELSE
      NULL
    END AS range_start,
    CASE WHEN (dim.column_type = 'TIMESTAMP'::regtype
      OR dim.column_type = 'TIMESTAMPTZ'::regtype
      OR dim.column_type = 'DATE'::regtype) THEN
      _timescaledb_internal.to_timestamp(dimsl.range_end)
    ELSE
      NULL
    END AS range_end,
    CASE WHEN (dim.column_type = 'TIMESTAMP'::regtype
      OR dim.column_type = 'TIMESTAMPTZ'::regtype
      OR dim.column_type = 'DATE'::regtype) THEN
      NULL
    ELSE
      dimsl.range_start
    END AS integer_range_start,
    CASE WHEN (dim.column_type = 'TIMESTAMP'::regtype
      OR dim.column_type = 'TIMESTAMPTZ'::regtype
      OR dim.column_type = 'DATE'::regtype) THEN
      NULL
    ELSE
      dimsl.range_end
    END AS integer_range_end,
    CASE WHEN (srcch.status & 1 = 1) THEN --distributed compress_chunk() has definitely been called
                                          --remote chunk compression status still uncertain
        TRUE
    ELSE FALSE --remote chunk compression status uncertain
    END AS is_compressed,
    pgtab.spcname AS chunk_table_space,
    chdn.node_list
  FROM _timescaledb_catalog.chunk srcch
    INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = srcch.hypertable_id
    INNER JOIN _timescaledb_catalog.chunk_constraint chcons ON srcch.id = chcons.chunk_id
    INNER JOIN _timescaledb_catalog.dimension dim ON srcch.hypertable_id = dim.hypertable_id
    INNER JOIN _timescaledb_catalog.dimension_slice dimsl ON dim.id = dimsl.dimension_id
      AND chcons.dimension_slice_id = dimsl.id
    INNER JOIN (
      SELECT relname,
        reltablespace,
        nspname AS schema_name
      FROM pg_class,
        pg_namespace
      WHERE pg_class.relnamespace = pg_namespace.oid) cl ON srcch.table_name = cl.relname
      AND srcch.schema_name = cl.schema_name
    LEFT OUTER JOIN pg_tablespace pgtab ON pgtab.oid = reltablespace
  LEFT OUTER JOIN (
    SELECT chunk_id,
      array_agg(node_name ORDER BY node_name) AS node_list
    FROM _timescaledb_catalog.chunk_data_node
    GROUP BY chunk_id) chdn ON srcch.id = chdn.chunk_id
  WHERE srcch.dropped IS FALSE AND srcch.osm_chunk IS FALSE
    AND ht.compression_state != 2 ) finalq
WHERE chunk_dimension_num = 1;

-- hypertable's dimension information
-- CTEs aren't used in the query as PG does not always optimize them
-- as expected.

CREATE OR REPLACE VIEW timescaledb_information.dimensions AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  rank() OVER (PARTITION BY hypertable_id ORDER BY dim.id) AS dimension_number,
  dim.column_name,
  dim.column_type,
  CASE WHEN dim.interval_length IS NULL THEN
    'Space'
  ELSE
    'Time'
  END AS dimension_type,
  CASE WHEN dim.interval_length IS NOT NULL THEN
    CASE WHEN dim.column_type = 'TIMESTAMP'::regtype
      OR dim.column_type = 'TIMESTAMPTZ'::regtype
      OR dim.column_type = 'DATE'::regtype THEN
      _timescaledb_internal.to_interval (dim.interval_length)
    ELSE
      NULL
    END
  END AS time_interval,
  CASE WHEN dim.interval_length IS NOT NULL THEN
    CASE WHEN dim.column_type = 'TIMESTAMP'::regtype
      OR dim.column_type = 'TIMESTAMPTZ'::regtype
      OR dim.column_type = 'DATE'::regtype THEN
      NULL
    ELSE
      dim.interval_length
    END
  END AS integer_interval,
  dim.integer_now_func,
  dim.num_slices AS num_partitions
FROM _timescaledb_catalog.hypertable ht,
  _timescaledb_catalog.dimension dim
WHERE dim.hypertable_id = ht.id;

---compression parameters information ---
CREATE OR REPLACE VIEW timescaledb_information.compression_settings AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  segq.attname,
  segq.segmentby_column_index,
  segq.orderby_column_index,
  segq.orderby_asc,
  segq.orderby_nullsfirst
FROM _timescaledb_catalog.hypertable_compression segq,
  _timescaledb_catalog.hypertable ht
WHERE segq.hypertable_id = ht.id
  AND (segq.segmentby_column_index IS NOT NULL
    OR segq.orderby_column_index IS NOT NULL)
ORDER BY table_name,
  segmentby_column_index,
  orderby_column_index;

-- troubleshooting job errors view
CREATE OR REPLACE VIEW timescaledb_information.job_errors AS
SELECT
    job_id,
    error_data ->> 'proc_schema' as proc_schema,
    error_data ->> 'proc_name' as proc_name,
    pid,
    start_time,
    finish_time,
    error_data ->> 'sqlerrcode' AS sqlerrcode,
    CASE WHEN error_data ->>'message' IS NOT NULL THEN
      CASE WHEN error_data ->>'detail' IS NOT NULL THEN
        CASE WHEN error_data ->>'hint' IS NOT NULL THEN concat(error_data ->>'message', '. ', error_data ->>'detail', '. ', error_data->>'hint')
        ELSE concat(error_data ->>'message', ' ', error_data ->>'detail')
        END
      ELSE
        CASE WHEN error_data ->>'hint' IS NOT NULL THEN concat(error_data ->>'message', '. ', error_data->>'hint')
        ELSE error_data ->>'message'
        END
      END
    ELSE
      'job crash detected, see server logs'
    END
    AS err_message
FROM
    _timescaledb_internal.job_errors;

GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;

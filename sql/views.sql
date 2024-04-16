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
    WHERE ch.hypertable_id = ht.id AND ch.dropped IS FALSE AND ch.osm_chunk IS FALSE) AS num_chunks,
  (
    CASE WHEN compression_state = 1 THEN
      TRUE
    ELSE
      FALSE
    END) AS compression_enabled,
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
  creation_time AS chunk_creation_time
FROM (
  SELECT ht.schema_name AS hypertable_schema,
    ht.table_name AS hypertable_name,
    srcch.schema_name AS schema_name,
    srcch.table_name AS chunk_name,
    dim.column_name AS primary_dimension,
    dim.column_type AS primary_dimension_type,
    row_number() OVER (PARTITION BY chcons.chunk_id ORDER BY dim.id) AS chunk_dimension_num,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      _timescaledb_functions.to_timestamp(dimsl.range_start)
    ELSE
      NULL
    END AS range_start,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      _timescaledb_functions.to_timestamp(dimsl.range_end)
    ELSE
      NULL
    END AS range_end,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      NULL
    ELSE
      dimsl.range_start
    END AS integer_range_start,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      NULL
    ELSE
      dimsl.range_end
    END AS integer_range_end,
    CASE WHEN (srcch.status & 1 = 1) THEN
        TRUE
    ELSE FALSE
    END AS is_compressed,
    pgtab.spcname AS chunk_table_space,
	srcch.creation_time AS creation_time
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
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      _timescaledb_functions.to_interval(dim.interval_length)
    ELSE
      NULL
    END
  END AS time_interval,
  CASE WHEN dim.interval_length IS NOT NULL THEN
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
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
SELECT
	schema_name AS hypertable_schema,
  table_name AS hypertable_name,
  (unnest(cs.segmentby))::name COLLATE "C" AS attname,
  generate_series(1,array_length(cs.segmentby,1))::smallint AS segmentby_column_index,
  NULL::smallint AS orderby_column_index,
  NULL::bool AS orderby_asc,
  NULL::bool AS orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I',ht.schema_name,ht.table_name)::regclass AND cs.segmentby IS NOT NULL
WHERE compressed_hypertable_id IS NOT NULL
UNION ALL
SELECT
	schema_name AS hypertable_schema,
  table_name AS hypertable_name,
  (unnest(cs.orderby))::name COLLATE "C" AS attname,
  NULL::smallint AS segmentby_column_index,
  generate_series(1,array_length(cs.orderby,1))::smallint AS orderby_column_index,
  unnest(array_replace(array_replace(array_replace(cs.orderby_desc,false,NULL),true,false),NULL,true)) AS orderby_asc,
  unnest(cs.orderby_nullsfirst) AS orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I',ht.schema_name,ht.table_name)::regclass AND cs.orderby IS NOT NULL
WHERE compressed_hypertable_id IS NOT NULL
ORDER BY hypertable_name,
  segmentby_column_index,
  orderby_column_index;

-- Job errors view that adds a security barrier on the bgw_job_stat_history
-- table in _timescaledb_internal. The view only allows users to view
-- log entries belonging to jobs that are owned by any of the users
-- role. A special case is added so that the superuser or the database
-- owner can see all job log entries, even those that do not have an
-- associated job.
--
-- Note that we have to use a sub-select here since pg_database_owner
-- does not exist before PostgreSQL 14.
CREATE OR REPLACE VIEW timescaledb_information.job_errors
WITH (security_barrier = true) AS
SELECT
    job_id,
    data->'job'->>'proc_schema' as proc_schema,
    data->'job'->>'proc_name' as proc_name,
    pid,
    execution_start AS start_time,
    execution_finish AS finish_time,
    data->'error_data'->>'sqlerrcode' AS sqlerrcode,
    CASE WHEN data->'error_data'->>'message' IS NOT NULL THEN
      CASE WHEN data->'error_data'->>'detail' IS NOT NULL THEN
        CASE WHEN data->'error_data'->>'hint' IS NOT NULL THEN concat(data->'error_data'->>'message', '. ', data->'error_data'->>'detail', '. ', data->'error_data'->>'hint')
        ELSE concat(data->'error_data'->>'message', ' ', data->'error_data'->>'detail')
        END
      ELSE
        CASE WHEN data->'error_data'->>'hint' IS NOT NULL THEN concat(data->'error_data'->>'message', '. ', data->'error_data'->>'hint')
        ELSE data->'error_data'->>'message'
        END
      END
    ELSE
      'job crash detected, see server logs'
    END
    AS err_message
FROM
    _timescaledb_internal.bgw_job_stat_history
LEFT JOIN
    _timescaledb_config.bgw_job ON (bgw_job.id = bgw_job_stat_history.job_id)
WHERE
    succeeded IS FALSE
    AND (pg_catalog.pg_has_role(current_user,
			   (SELECT pg_catalog.pg_get_userbyid(datdba)
			      FROM pg_catalog.pg_database
			     WHERE datname = current_database()),
			   'MEMBER') IS TRUE
    OR pg_catalog.pg_has_role(current_user, owner, 'MEMBER') IS TRUE);

CREATE OR REPLACE VIEW timescaledb_information.job_history
WITH (security_barrier = true) AS
SELECT
    h.id,
    h.job_id,
    h.succeeded,
    coalesce(h.data->'job'->>'proc_schema', j.proc_schema) as proc_schema,
    coalesce(h.data->'job'->>'proc_name', j.proc_name) as proc_name,
    h.pid,
    h.execution_start AS start_time,
    h.execution_finish AS finish_time,
    h.data->'job'->'config' AS config,
    h.data->'error_data'->>'sqlerrcode' AS sqlerrcode,
    CASE
      WHEN h.succeeded IS FALSE AND h.data->'error_data'->>'message' IS NOT NULL THEN
        CASE WHEN h.data->'error_data'->>'detail' IS NOT NULL THEN
          CASE WHEN h.data->'error_data'->>'hint' IS NOT NULL THEN concat(h.data->'error_data'->>'message', '. ', h.data->'error_data'->>'detail', '. ', h.data->'error_data'->>'hint')
          ELSE concat(h.data->'error_data'->>'message', ' ', h.data->'error_data'->>'detail')
          END
        ELSE
          CASE WHEN h.data->'error_data'->>'hint' IS NOT NULL THEN concat(h.data->'error_data'->>'message', '. ', h.data->'error_data'->>'hint')
          ELSE h.data->'error_data'->>'message'
          END
        END
      WHEN h.succeeded IS FALSE AND h.execution_finish IS NOT NULL THEN
        'job crash detected, see server logs'
      WHEN h.execution_finish IS NULL THEN
        E'job didn\'t finish yet'
    END AS err_message
FROM
    _timescaledb_internal.bgw_job_stat_history h
LEFT JOIN
    _timescaledb_config.bgw_job j ON (j.id = h.job_id)
WHERE (pg_catalog.pg_has_role(current_user,
			   (SELECT pg_catalog.pg_get_userbyid(datdba)
			      FROM pg_catalog.pg_database
			     WHERE datname = current_database()),
			   'MEMBER') IS TRUE
    OR pg_catalog.pg_has_role(current_user, owner, 'MEMBER') IS TRUE);

CREATE OR REPLACE VIEW timescaledb_information.hypertable_compression_settings AS
	SELECT
		format('%I.%I',ht.schema_name,ht.table_name)::regclass AS hypertable,
		array_to_string(segmentby,',') AS segmentby,
		un.orderby,
    d.compress_interval_length
  FROM _timescaledb_catalog.hypertable ht
  JOIN LATERAL (
    SELECT
      CASE WHEN d.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
        _timescaledb_functions.to_interval(d.compress_interval_length)::text
      ELSE
        d.compress_interval_length::text
      END AS compress_interval_length
    FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = ht.id ORDER BY id LIMIT 1
  ) d ON true
  LEFT JOIN _timescaledb_catalog.compression_settings s ON format('%I.%I',ht.schema_name,ht.table_name)::regclass = s.relid
	LEFT JOIN LATERAL (
		SELECT
			string_agg(
				format('%I%s%s',orderby,
					CASE WHEN "desc" THEN ' DESC' ELSE '' END,
					CASE WHEN nullsfirst AND NOT "desc" THEN ' NULLS FIRST' WHEN NOT nullsfirst AND "desc" THEN ' NULLS LAST' ELSE '' END
				)
			,',') AS orderby
		FROM unnest(s.orderby, s.orderby_desc, s.orderby_nullsfirst) un(orderby, "desc", nullsfirst)
	) un ON true;

CREATE OR REPLACE VIEW timescaledb_information.chunk_compression_settings AS
	SELECT
		format('%I.%I',ht.schema_name,ht.table_name)::regclass AS hypertable,
		format('%I.%I',ch.schema_name,ch.table_name)::regclass AS chunk,
		array_to_string(segmentby,',') AS segmentby,
		un.orderby
	FROM _timescaledb_catalog.hypertable ht
	INNER JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id
  INNER JOIN _timescaledb_catalog.chunk ch2 ON ch2.id = ch.compressed_chunk_id
  LEFT JOIN _timescaledb_catalog.compression_settings s ON format('%I.%I',ch2.schema_name,ch2.table_name)::regclass = s.relid
	LEFT JOIN LATERAL (
		SELECT
			string_agg(
				format('%I%s%s',orderby,
					CASE WHEN "desc" THEN ' DESC' ELSE '' END,
					CASE WHEN nullsfirst AND NOT "desc" THEN ' NULLS FIRST' WHEN NOT nullsfirst AND "desc" THEN ' NULLS LAST' ELSE '' END
				)
			,',') AS orderby
		FROM unnest(s.orderby, s.orderby_desc, s.orderby_nullsfirst) un(orderby, "desc", nullsfirst)
	) un ON true;

GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;


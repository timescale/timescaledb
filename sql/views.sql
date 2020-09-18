-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS timescaledb_information;

-- Convenience view to list all hypertables
CREATE OR REPLACE VIEW timescaledb_information.hypertables AS
  SELECT
    ht.schema_name AS table_schema,
    ht.table_name as table_name,
    t.tableowner AS owner,
    ht.num_dimensions,
    (SELECT count(1)
     FROM _timescaledb_catalog.chunk ch
     WHERE ch.hypertable_id=ht.id
    ) AS num_chunks,
    (CASE WHEN ht.compressed_hypertable_id IS NULL THEN false ELSE true END) AS
  compression_enabled,
    (CASE WHEN ht.replication_factor > 0 THEN true ELSE false END) AS is_distributed,
    ht.replication_factor ,
    dn.node_list as data_nodes,
    srchtbs.tablespace_list as tablespaces
  FROM _timescaledb_catalog.hypertable ht
        INNER JOIN pg_tables t
        ON ht.table_name=t.tablename
           AND ht.schema_name=t.schemaname
        LEFT OUTER JOIN _timescaledb_catalog.continuous_agg ca
        ON ca.mat_hypertable_id=ht.id
        LEFT OUTER JOIN (
            SELECT hypertable_id,
            array_agg(tablespace_name ORDER BY id) as tablespace_list
            FROM _timescaledb_catalog.tablespace
            GROUP BY hypertable_id ) srchtbs
       ON ht.id = srchtbs.hypertable_id
       LEFT OUTER JOIN (
            SELECT hypertable_id,
            array_agg(node_name ORDER BY node_name) as node_list
                      FROM _timescaledb_catalog.hypertable_data_node
                      GROUP BY hypertable_id) dn
    ON ht.id = dn.hypertable_id
    WHERE ht.compressed is false --> no internal compression tables
    AND ca.mat_hypertable_id IS NULL;

CREATE OR REPLACE VIEW timescaledb_information.license AS
  SELECT _timescaledb_internal.license_edition() as edition,
         _timescaledb_internal.license_expiration_time() <= now() AS expired,
         _timescaledb_internal.license_expiration_time() AS expiration_time;

CREATE OR REPLACE VIEW timescaledb_information.policy_stats as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, j.id AS job_id, 
   js.last_start as last_run_started_at, 
   js.last_successful_finish as last_successful_finish, 
   CASE WHEN js.last_finish < '4714-11-24 00:00:00+00 BC' THEN NULL
        WHEN js.last_finish IS NOT NULL THEN
              CASE WHEN js.last_run_success = 't' THEN 'Success'
                   WHEN js.last_run_success = 'f' THEN 'Failed'
              END
    END as last_run_status,
    CASE WHEN js.last_finish < '4714-11-24 00:00:00+00 BC' THEN 'Running'
         WHEN js.next_start IS NOT NULL THEN 'Scheduled'
    END as job_status,
    CASE WHEN js.last_finish > js.last_start THEN (js.last_finish - js.last_start)
    END as last_run_duration,
    js.next_start as next_scheduled_run,
    js.total_runs, js.total_successes, js.total_failures
  FROM _timescaledb_config.bgw_job j
    INNER JOIN _timescaledb_catalog.hypertable ht ON j.hypertable_id = ht.id
    INNER JOIN _timescaledb_internal.bgw_job_stat js on j.id = js.job_id
  ORDER BY ht.schema_name, ht.table_name;

-- views for continuous aggregate queries ---
CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregates as
  SELECT format('%1$I.%2$I', cagg.user_view_schema, cagg.user_view_name)::regclass as view_name,
    viewinfo.viewowner as view_owner,
    bgwjob.schedule_interval,
    cagg.materialized_only,
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as materialization_hypertable,
    directview.viewdefinition as view_definition
  FROM  _timescaledb_catalog.continuous_agg cagg
        LEFT JOIN _timescaledb_config.bgw_job bgwjob ON bgwjob.hypertable_id = cagg.mat_hypertable_id,
        _timescaledb_catalog.hypertable ht, LATERAL
        ( select C.oid, pg_get_userbyid( C.relowner) as viewowner
          FROM pg_class C LEFT JOIN pg_namespace N on (N.oid = C.relnamespace)
          where C.relkind = 'v' and C.relname = cagg.user_view_name
          and N.nspname = cagg.user_view_schema ) viewinfo, LATERAL
        ( select pg_get_viewdef(C.oid) as viewdefinition
          FROM pg_class C LEFT JOIN pg_namespace N on (N.oid = C.relnamespace)
          where C.relkind = 'v' and C.relname = cagg.direct_view_name
          and N.nspname = cagg.direct_view_schema ) directview
  WHERE cagg.mat_hypertable_id = ht.id;

CREATE OR REPLACE VIEW timescaledb_information.data_node AS
  SELECT s.node_name, s.owner, s.options
  FROM (SELECT srvname AS node_name, srvowner::regrole::name AS owner, srvoptions AS options
        FROM pg_catalog.pg_foreign_server AS srv, pg_catalog.pg_foreign_data_wrapper AS fdw
        WHERE srv.srvfdw = fdw.oid
        AND fdw.fdwname = 'timescaledb_fdw') AS s;

-- chunks metadata view, shows information about the primary dimension column
-- query plans with CTEs are not always optimized by PG. So use in-line
-- tables.
CREATE OR REPLACE VIEW  timescaledb_information.chunks
AS
SELECT 
       hypertable_schema, hypertable_name,
       schema_name as chunk_schema , chunk_name ,
       primary_dimension, primary_dimension_type,
       range_start, range_end,
       integer_range_start as range_start_integer,
       integer_range_end as range_end_integer,
       is_compressed,
       chunk_table_space as chunk_tablespace,
       node_list as data_nodes
       from
(
SELECT
    ht.schema_name as hypertable_schema,
    ht.table_name as hypertable_name,
    srcch.schema_name as schema_name,
    srcch.table_name as chunk_name,
    dim.column_name as primary_dimension,
    dim.column_type as primary_dimension_type,
    row_number() over(partition by chcons.chunk_id order by dim.id) as chunk_dimension_num,
    CASE
        WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
               dim.column_type = 'TIMESTAMPTZ'::regtype OR
               dim.column_type = 'DATE'::regtype )
        THEN _timescaledb_internal.to_timestamp(dimsl.range_start)
        ELSE NULL
    END as range_start,
    CASE
         WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
                dim.column_type = 'TIMESTAMPTZ'::regtype OR
                dim.column_type = 'DATE'::regtype )
         THEN _timescaledb_internal.to_timestamp(dimsl.range_end)
         ELSE NULL
    END as range_end,
    CASE
        WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
               dim.column_type = 'TIMESTAMPTZ'::regtype OR
               dim.column_type = 'DATE'::regtype )
        THEN NULL
        ELSE dimsl.range_start
    END as integer_range_start,
    CASE
        WHEN ( dim.column_type = 'TIMESTAMP'::regtype OR
               dim.column_type = 'TIMESTAMPTZ'::regtype OR
               dim.column_type = 'DATE'::regtype )
        THEN NULL
        ELSE dimsl.range_end
    END as integer_range_end,
    CASE WHEN srcch.compressed_chunk_id is not null THEN true
         ELSE false
    END as is_compressed,
    pgtab.spcname as chunk_table_space,
    chdn.node_list
FROM _timescaledb_catalog.chunk srcch 
      INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = srcch.hypertable_id
      INNER JOIN _timescaledb_catalog.chunk_constraint chcons ON srcch.id = chcons.chunk_id
      INNER JOIN _timescaledb_catalog.dimension dim ON srcch.hypertable_id = dim.hypertable_id
      INNER JOIN _timescaledb_catalog.dimension_slice dimsl ON dim.id = dimsl.dimension_id
                                and chcons.dimension_slice_id = dimsl.id
      INNER JOIN
       ( SELECT relname, reltablespace, nspname as schema_name 
                FROM pg_class , pg_namespace WHERE
                pg_class.relnamespace = pg_namespace.oid) cl 
       ON srcch.table_name = cl.relname and srcch.schema_name = cl.schema_name
       LEFT OUTER JOIN pg_tablespace pgtab ON pgtab.oid = reltablespace
       left outer join (
               SELECT chunk_id, array_agg(node_name ORDER BY node_name) as node_list
                            FROM _timescaledb_catalog.chunk_data_node
                            GROUP BY chunk_id) chdn
       ON srcch.id = chdn.chunk_id 
 WHERE srcch.dropped is false 
      and ht.compressed = false ) finalq
WHERE chunk_dimension_num = 1
;

-- hypertable's dimension information
-- CTEs aren't used in the query as PG does not always optimize them
-- as expected.
CREATE OR REPLACE VIEW timescaledb_information.dimensions
AS
SELECT 
    ht.schema_name as hypertable_schema,
    ht.table_name as hypertable_name,
    rank() over(partition by hypertable_id order by dim.id) as dimension_number,
    dim.column_name,
    dim.column_type,
    CASE WHEN dim.interval_length is NULL 
         THEN 'Space' 
         ELSE 'Time' 
    END as dimension_type,
    CASE WHEN dim.interval_length is NOT NULL THEN
              CASE 
              WHEN dim.column_type = 'TIMESTAMP'::regtype OR 
                   dim.column_type = 'TIMESTAMPTZ'::regtype OR 
                   dim.column_type = 'DATE'::regtype 
              THEN _timescaledb_internal.to_interval(dim.interval_length) 
              ELSE NULL 
              END
    END as time_interval, 
    CASE WHEN dim.interval_length is NOT NULL THEN
              CASE 
              WHEN dim.column_type = 'TIMESTAMP'::regtype OR 
                   dim.column_type = 'TIMESTAMPTZ'::regtype OR 
                   dim.column_type = 'DATE'::regtype 
              THEN  NULL
              ELSE dim.interval_length 
              END
    END as integer_interval, 
    dim.integer_now_func,
    dim.num_slices as num_partitions
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.dimension dim
WHERE dim.hypertable_id = ht.id
;

---compression parameters information ---
CREATE VIEW timescaledb_information.compression_settings
AS
SELECT
    ht.schema_name,
    ht.table_name,
    segq.attname,
    segq.segmentby_column_index,
    segq.orderby_column_index,
    segq.orderby_asc,
    segq.orderby_nullsfirst
FROM
    _timescaledb_catalog.hypertable_compression segq,
    _timescaledb_catalog.hypertable ht
WHERE
    segq.hypertable_id = ht.id
    AND ( segq.segmentby_column_index IS NOT NULL
        OR segq.orderby_column_index IS NOT NULL)
ORDER BY
    table_name,
    segmentby_column_index,
    orderby_column_index;

GRANT USAGE ON SCHEMA timescaledb_information TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;



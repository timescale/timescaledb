-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS timescaledb_information;

-- Convenience view to list all hypertables and their space usage
CREATE OR REPLACE VIEW timescaledb_information.hypertable_size_info AS
  SELECT ht.id, ht.schema_name AS table_schema,
    ht.table_name,
    t.tableowner AS table_owner,
    ht.num_dimensions,
    (SELECT count(1)
     FROM _timescaledb_catalog.chunk ch
     WHERE ch.hypertable_id=ht.id
    ) AS num_chunks,
    (CASE WHEN ht.replication_factor > 0 THEN true ELSE false END) AS distributed,
    bsize.table_bytes,
    bsize.index_bytes,
    bsize.toast_bytes,
    bsize.total_bytes
  FROM _timescaledb_catalog.hypertable ht
    LEFT OUTER JOIN pg_tables t ON ht.table_name=t.tablename AND ht.schema_name=t.schemaname
    LEFT OUTER JOIN LATERAL @extschema@.hypertable_relation_size(
      CASE WHEN has_schema_privilege(ht.schema_name,'USAGE') THEN format('%I.%I',ht.schema_name,ht.table_name) ELSE NULL END
    ) bsize ON true;

-- Convenience view to list all hypertables and their space usage
CREATE OR REPLACE VIEW timescaledb_information.hypertable AS
WITH ht_size as (
  SELECT ht.id, ht.table_schema,
    ht.table_name,
    ht.table_owner,
    ht.num_dimensions,
    (SELECT count(1)
     FROM _timescaledb_catalog.chunk ch
     WHERE ch.hypertable_id=ht.id
    ) AS num_chunks,
    ht.table_bytes,
    ht.index_bytes,
    ht.toast_bytes,
    ht.total_bytes,
    ht.distributed
  FROM timescaledb_information.hypertable_size_info ht
),
compht_size as
(
  select srcht.id,
  sum(map.compressed_heap_size) as heap_bytes,
  sum(map.compressed_index_size) as index_bytes,
  sum(map.compressed_toast_size) as toast_bytes,
  sum(map.compressed_heap_size) + sum(map.compressed_toast_size) + sum(map.compressed_index_size) as total_bytes
 FROM _timescaledb_catalog.chunk srcch, _timescaledb_catalog.compression_chunk_size map,
      _timescaledb_catalog.hypertable srcht
 where map.chunk_id = srcch.id and srcht.id = srcch.hypertable_id
 group by srcht.id
)
select hts.table_schema, hts.table_name, hts.table_owner,
       hts.num_dimensions, hts.num_chunks,
       pg_size_pretty( COALESCE(hts.table_bytes + compht_size.heap_bytes, hts.table_bytes)) as table_size,
       pg_size_pretty( COALESCE(hts.index_bytes + compht_size.index_bytes , hts.index_bytes, compht_size.index_bytes)) as index_size,
       pg_size_pretty( COALESCE(hts.toast_bytes + compht_size.toast_bytes, hts.toast_bytes, compht_size.toast_bytes)) as toast_size,
       pg_size_pretty( COALESCE(hts.total_bytes + compht_size.total_bytes, hts.total_bytes)) as total_size,
       hts.distributed
FROM ht_size hts LEFT OUTER JOIN compht_size
ON hts.id = compht_size.id;

CREATE OR REPLACE VIEW timescaledb_information.license AS
  SELECT _timescaledb_internal.license_edition() as edition,
         _timescaledb_internal.license_expiration_time() <= now() AS expired,
         _timescaledb_internal.license_expiration_time() AS expiration_time;

CREATE OR REPLACE VIEW timescaledb_information.drop_chunks_policies as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.older_than, p.job_id, j.schedule_interval,
    j.max_runtime, j.max_retries, j.retry_period, p.cascade_to_materializations
  FROM _timescaledb_config.bgw_policy_drop_chunks p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id;

CREATE OR REPLACE VIEW timescaledb_information.reorder_policies AS
SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass AS hypertable,
  jsonb_object_field_text (j.config, 'index_name') AS hypertable_index_name,
  j.id AS job_id,
  j.schedule_interval,
  j.max_runtime,
  j.max_retries,
  j.retry_period
FROM _timescaledb_config.bgw_job j
  INNER JOIN _timescaledb_catalog.hypertable ht ON j.hypertable_id = ht.id
WHERE job_type = 'reorder';


CREATE OR REPLACE VIEW timescaledb_information.policy_stats as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.job_id, j.job_type, js.last_run_success, js.last_finish, js.last_successful_finish, js.last_start, js.next_start,
    js.total_runs, js.total_failures
  FROM (SELECT id AS job_id, hypertable_id FROM _timescaledb_config.bgw_job WHERE job_type IN ('reorder')
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_drop_chunks
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_compress_chunks
        UNION SELECT job_id, raw_hypertable_id FROM _timescaledb_catalog.continuous_agg) p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id
    INNER JOIN _timescaledb_internal.bgw_job_stat js on p.job_id = js.job_id
  ORDER BY ht.schema_name, ht.table_name;

-- views for continuous aggregate queries ---
CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregates as
  SELECT format('%1$I.%2$I', cagg.user_view_schema, cagg.user_view_name)::regclass as view_name,
    viewinfo.viewowner as view_owner,
    CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
      WHEN 'TIMESTAMP'::regtype
        THEN _timescaledb_internal.to_interval(cagg.refresh_lag)::TEXT
      WHEN 'TIMESTAMPTZ'::regtype
        THEN _timescaledb_internal.to_interval(cagg.refresh_lag)::TEXT
      WHEN 'DATE'::regtype
        THEN _timescaledb_internal.to_interval(cagg.refresh_lag)::TEXT
      ELSE cagg.refresh_lag::TEXT
    END AS refresh_lag,
    bgwjob.schedule_interval as refresh_interval,
    CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
      WHEN 'TIMESTAMP'::regtype
        THEN _timescaledb_internal.to_interval(cagg.max_interval_per_job)::TEXT
      WHEN 'TIMESTAMPTZ'::regtype
        THEN _timescaledb_internal.to_interval(cagg.max_interval_per_job)::TEXT
      WHEN 'DATE'::regtype
        THEN _timescaledb_internal.to_interval(cagg.max_interval_per_job)::TEXT
      ELSE cagg.max_interval_per_job::TEXT
    END AS max_interval_per_job,
    CASE
      WHEN cagg.ignore_invalidation_older_than = BIGINT '9223372036854775807'
        THEN NULL
      ELSE
    CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
          WHEN 'TIMESTAMP'::regtype
            THEN _timescaledb_internal.to_interval(cagg.ignore_invalidation_older_than)::TEXT
          WHEN 'TIMESTAMPTZ'::regtype
            THEN _timescaledb_internal.to_interval(cagg.ignore_invalidation_older_than)::TEXT
          WHEN 'DATE'::regtype
            THEN _timescaledb_internal.to_interval(cagg.ignore_invalidation_older_than)::TEXT
          ELSE cagg.ignore_invalidation_older_than::TEXT
        END
    END AS ignore_invalidation_older_than,
    cagg.materialized_only,
    format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as materialization_hypertable,
    directview.viewdefinition as view_definition
  FROM  _timescaledb_catalog.continuous_agg cagg,
        _timescaledb_catalog.hypertable ht, LATERAL
        ( select C.oid, pg_get_userbyid( C.relowner) as viewowner
          FROM pg_class C LEFT JOIN pg_namespace N on (N.oid = C.relnamespace)
          where C.relkind = 'v' and C.relname = cagg.user_view_name
          and N.nspname = cagg.user_view_schema ) viewinfo, LATERAL
        ( select schedule_interval
          FROM  _timescaledb_config.bgw_job
          where id = cagg.job_id ) bgwjob, LATERAL
        ( select pg_get_viewdef(C.oid) as viewdefinition
          FROM pg_class C LEFT JOIN pg_namespace N on (N.oid = C.relnamespace)
          where C.relkind = 'v' and C.relname = cagg.direct_view_name
          and N.nspname = cagg.direct_view_schema ) directview
  WHERE cagg.mat_hypertable_id = ht.id;

CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregate_stats as
  SELECT format('%1$I.%2$I', cagg.user_view_schema, cagg.user_view_name)::regclass as view_name,
    CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
      WHEN 'TIMESTAMP'::regtype
        THEN _timescaledb_internal.to_timestamp_without_timezone(ct.watermark)::TEXT
      WHEN 'TIMESTAMPTZ'::regtype
        THEN _timescaledb_internal.to_timestamp(ct.watermark)::TEXT
      WHEN 'DATE'::regtype
        THEN _timescaledb_internal.to_date(ct.watermark)::TEXT
      ELSE ct.watermark::TEXT
    END AS completed_threshold,
    CASE _timescaledb_internal.get_time_type(cagg.raw_hypertable_id)
      WHEN 'TIMESTAMP'::regtype
        THEN _timescaledb_internal.to_timestamp_without_timezone(it.watermark)::TEXT
      WHEN 'TIMESTAMPTZ'::regtype
        THEN _timescaledb_internal.to_timestamp(it.watermark)::TEXT
      WHEN 'DATE'::regtype
        THEN _timescaledb_internal.to_date(it.watermark)::TEXT
      ELSE it.watermark::TEXT
    END AS invalidation_threshold,
    cagg.job_id as job_id,
    bgw_job_stat.last_start as last_run_started_at,
    bgw_job_stat.last_successful_finish as last_successful_finish,
    CASE WHEN bgw_job_stat.last_finish < '4714-11-24 00:00:00+00 BC' THEN NULL
         WHEN bgw_job_stat.last_finish IS NOT NULL THEN
              CASE WHEN bgw_job_stat.last_run_success = 't' THEN 'Success'
                   WHEN bgw_job_stat.last_run_success = 'f' THEN 'Failed'
              END
    END as last_run_status,
    CASE WHEN bgw_job_stat.last_finish < '4714-11-24 00:00:00+00 BC' THEN 'Running'
       WHEN bgw_job_stat.next_start IS NOT NULL THEN 'Scheduled'
    END as job_status,
    CASE WHEN bgw_job_stat.last_finish > bgw_job_stat.last_start THEN (bgw_job_stat.last_finish - bgw_job_stat.last_start)
    END as last_run_duration,
    bgw_job_stat.next_start as next_scheduled_run,
    bgw_job_stat.total_runs,
    bgw_job_stat.total_successes,
    bgw_job_stat.total_failures,
    bgw_job_stat.total_crashes
  FROM
    _timescaledb_catalog.continuous_agg as cagg
    LEFT JOIN _timescaledb_internal.bgw_job_stat as bgw_job_stat
    ON  ( cagg.job_id = bgw_job_stat.job_id )
    LEFT JOIN _timescaledb_catalog.continuous_aggs_invalidation_threshold as it
    ON ( cagg.raw_hypertable_id = it.hypertable_id)
    LEFT JOIN _timescaledb_catalog.continuous_aggs_completed_threshold as ct
    ON ( cagg.mat_hypertable_id = ct.materialization_id);

CREATE OR REPLACE VIEW  timescaledb_information.compressed_chunk_stats
AS
WITH mapq as
(select
  chunk_id,
  pg_size_pretty(map.uncompressed_heap_size) as uncompressed_heap_bytes,
  pg_size_pretty(map.uncompressed_index_size) as uncompressed_index_bytes,
  pg_size_pretty(map.uncompressed_toast_size) as uncompressed_toast_bytes,
  pg_size_pretty(map.uncompressed_heap_size + map.uncompressed_toast_size + map.uncompressed_index_size) as uncompressed_total_bytes,
  pg_size_pretty(map.compressed_heap_size) as compressed_heap_bytes,
  pg_size_pretty(map.compressed_index_size) as compressed_index_bytes,
  pg_size_pretty(map.compressed_toast_size) as compressed_toast_bytes,
  pg_size_pretty(map.compressed_heap_size + map.compressed_toast_size + map.compressed_index_size) as compressed_total_bytes
 FROM _timescaledb_catalog.compression_chunk_size map )
  SELECT format('%1$I.%2$I', srcht.schema_name, srcht.table_name)::regclass as hypertable_name,
  format('%1$I.%2$I', srcch.schema_name, srcch.table_name)::regclass as chunk_name,
  CASE WHEN srcch.compressed_chunk_id IS NULL THEN 'Uncompressed'::TEXT ELSE 'Compressed'::TEXT END as compression_status,
  mapq.uncompressed_heap_bytes,
  mapq.uncompressed_index_bytes,
  mapq.uncompressed_toast_bytes,
  mapq.uncompressed_total_bytes,
  mapq.compressed_heap_bytes,
  mapq.compressed_index_bytes,
  mapq.compressed_toast_bytes,
  mapq.compressed_total_bytes
  FROM _timescaledb_catalog.hypertable as srcht JOIN _timescaledb_catalog.chunk as srcch
  ON srcht.id = srcch.hypertable_id and srcht.compressed_hypertable_id IS NOT NULL and srcch.dropped = false
  LEFT JOIN mapq
  ON srcch.id = mapq.chunk_id ;

CREATE OR REPLACE VIEW  timescaledb_information.compressed_hypertable_stats
AS
  SELECT format('%1$I.%2$I', srcht.schema_name, srcht.table_name)::regclass as hypertable_name,
  ( select count(*) from _timescaledb_catalog.chunk where hypertable_id = srcht.id) as total_chunks,
  count(*) as number_compressed_chunks,
  pg_size_pretty(sum(map.uncompressed_heap_size)) as uncompressed_heap_bytes,
  pg_size_pretty(sum(map.uncompressed_index_size)) as uncompressed_index_bytes,
  pg_size_pretty(sum(map.uncompressed_toast_size)) as uncompressed_toast_bytes,
  pg_size_pretty(sum(map.uncompressed_heap_size) + sum(map.uncompressed_toast_size) + sum(map.uncompressed_index_size)) as uncompressed_total_bytes,
  pg_size_pretty(sum(map.compressed_heap_size)) as compressed_heap_bytes,
  pg_size_pretty(sum(map.compressed_index_size)) as compressed_index_bytes,
  pg_size_pretty(sum(map.compressed_toast_size)) as compressed_toast_bytes,
  pg_size_pretty(sum(map.compressed_heap_size) + sum(map.compressed_toast_size) + sum(map.compressed_index_size)) as compressed_total_bytes
 FROM _timescaledb_catalog.chunk srcch, _timescaledb_catalog.compression_chunk_size map,
      _timescaledb_catalog.hypertable srcht
 where map.chunk_id = srcch.id and srcht.id = srcch.hypertable_id
 group by srcht.id;

CREATE OR REPLACE VIEW timescaledb_information.data_node AS
  SELECT s.node_name, s.owner, s.options, s.node_up,
    COUNT(size.table_name) AS num_dist_tables,
    SUM(size.num_chunks) AS num_dist_chunks,
    pg_size_pretty(SUM(size.total_bytes)) AS total_dist_size
  FROM (SELECT srvname AS node_name, srvowner::regrole::name AS owner, srvoptions AS options, _timescaledb_internal.ping_data_node(srvname) AS node_up
        FROM pg_catalog.pg_foreign_server AS srv, pg_catalog.pg_foreign_data_wrapper AS fdw
        WHERE srv.srvfdw = fdw.oid
        AND fdw.fdwname = 'timescaledb_fdw') AS s
    LEFT OUTER JOIN LATERAL @extschema@.data_node_hypertable_info(CASE WHEN s.node_up THEN s.node_name ELSE NULL END) size ON TRUE
  GROUP BY s.node_name, s.node_up, s.owner, s.options;

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
    row_number() over(partition by chcons.chunk_id order by chcons.dimension_slice_id) as chunk_dimension_num,
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
    CASE WHEN srcch.compressed_chunk_id is not null THEN 'true'
         ELSE 'false'
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



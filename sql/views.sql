-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS timescaledb_information;

-- Convenience view to list all hypertables and their space usage
CREATE OR REPLACE VIEW timescaledb_information.hypertable AS
  SELECT ht.schema_name AS table_schema,
    ht.table_name,
    t.tableowner AS table_owner,
    ht.num_dimensions,
    (SELECT count(1)
     FROM _timescaledb_catalog.chunk ch
     WHERE ch.hypertable_id=ht.id
    ) AS num_chunks,
    size.table_size,
    size.index_size,
    size.toast_size,
    size.total_size
  FROM _timescaledb_catalog.hypertable ht
    LEFT OUTER JOIN pg_tables t ON ht.table_name=t.tablename AND ht.schema_name=t.schemaname
    LEFT OUTER JOIN LATERAL @extschema@.hypertable_relation_size_pretty(
      CASE WHEN has_schema_privilege(ht.schema_name,'USAGE') THEN format('%I.%I',ht.schema_name,ht.table_name) ELSE NULL END
    ) size ON true;

CREATE OR REPLACE VIEW timescaledb_information.license AS
  SELECT _timescaledb_internal.license_edition() as edition,
         _timescaledb_internal.license_expiration_time() <= now() AS expired,
         _timescaledb_internal.license_expiration_time() AS expiration_time;

CREATE OR REPLACE VIEW timescaledb_information.drop_chunks_policies as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.older_than, p.cascade, p.job_id, j.schedule_interval,
    j.max_runtime, j.max_retries, j.retry_period, p.cascade_to_materializations
  FROM _timescaledb_config.bgw_policy_drop_chunks p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id;

CREATE OR REPLACE VIEW timescaledb_information.reorder_policies as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.hypertable_index_name, p.job_id, j.schedule_interval,
    j.max_runtime, j.max_retries, j.retry_period
  FROM _timescaledb_config.bgw_policy_reorder p
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id;

CREATE OR REPLACE VIEW timescaledb_information.policy_stats as
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.job_id, j.job_type, js.last_run_success, js.last_finish, js.last_successful_finish, js.last_start, js.next_start,
    js.total_runs, js.total_failures
  FROM (SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_reorder
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_drop_chunks) p
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
    CASE when bgw_job_stat.last_run_success = 't' then 'Success'
         when bgw_job_stat.last_run_success = 'f' then 'Failed'
    END AS last_run_status,
    case when bgw_job_stat.last_finish < '4714-11-24 00:00:00+00 BC' then 'running'
       when bgw_job_stat.next_start is not null then 'scheduled'
    end as job_status,
    case when bgw_job_stat.last_finish > bgw_job_stat.last_start then (bgw_job_stat.last_finish - bgw_job_stat.last_start)
    end as last_run_duration,
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
  ON srcht.id = srcch.hypertable_id and srcht.compressed_hypertable_id IS NOT NULL
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

GRANT USAGE ON SCHEMA timescaledb_information TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;

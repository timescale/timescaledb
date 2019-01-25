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
    j.max_runtime, j.max_retries, j.retry_period
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
  SELECT format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass as hypertable, p.job_id, j.job_type, js.last_run_success, js.last_finish, js.last_start, js.next_start, 
    js.total_runs, js.total_failures 
  FROM (SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_reorder 
        UNION SELECT job_id, hypertable_id FROM _timescaledb_config.bgw_policy_drop_chunks) p  
    INNER JOIN _timescaledb_catalog.hypertable ht ON p.hypertable_id = ht.id
    INNER JOIN _timescaledb_config.bgw_job j ON p.job_id = j.id
    INNER JOIN _timescaledb_internal.bgw_job_stat js on p.job_id = js.job_id
  ORDER BY ht.schema_name, ht.table_name;

GRANT USAGE ON SCHEMA timescaledb_information TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;


DROP VIEW IF EXISTS timescaledb_new.policies;

CREATE VIEW timescaledb_new.compression_info
AS
  SELECT ht.schema_name, ht.table_name, ordq.hypertable_id, segq.segmentby_columns, ordq.orderby_columns 
  FROM
  (
    SELECT hypertable_id, CASE WHEN orderby_columns IS NOT NULL THEN orderby_columns ELSE NULL END as orderby_columns
    FROM
    (
        SELECT hypertable_id, array_agg(attname order by orderby_column_index) orderby_columns
        FROM (SELECT hypertable_id , attname, orderby_column_index FROM _timescaledb_catalog.hypertable_compression
              where orderby_column_index is not null) foo GROUP BY hypertable_id 
    ) ordsubq 
  ) ordq
  LEFT OUTER JOIN 
  (
    SELECT hypertable_id, CASE WHEN segmentby_columns IS NOT NULL THEN segmentby_columns ELSE NULL END as segmentby_columns
    FROM
    (
        SELECT hypertable_id, array_agg(attname order by segmentby_column_index) segmentby_columns 
        FROM (SELECT hypertable_id , attname, segmentby_column_index FROM _timescaledb_catalog.hypertable_compression 
              where segmentby_column_index is not null) foo GROUP BY hypertable_id 
    ) segsubq 
  ) segq
ON segq.hypertable_id = ordq.hypertable_id
JOIN _timescaledb_catalog.hypertable ht ON ordq.hypertable_id = ht.id;


CREATE VIEW timescaledb_new.policies
AS
 SELECT 
    ht.schema_name,
    ht.table_name,
    job.id,
    job.application_name,
    job.job_type,
    job.schedule_interval,
    job.max_runtime,
    job.max_retries,
    job.retry_period,
    jsonb_build_object(
            'schema_name', ht.schema_name,
            'name', ht.table_name,
            'segmentby_columns', ht.segmentby_columns, 
            'orderby_columns', ht.orderby_columns, 
            'older_than', d.older_than) AS jsonb_build_object
   FROM _timescaledb_config.bgw_job job,
        _timescaledb_config.bgw_policy_compress_chunks d,
        timescaledb_new.compression_info ht
  WHERE job.id = d.job_id and ht.hypertable_id = d.hypertable_id
UNION
 SELECT 
    ht.schema_name,
    ht.table_name,
    job.id,
    job.application_name,
    job.job_type,
    job.schedule_interval,
    job.max_runtime,
    job.max_retries,
    job.retry_period,
    jsonb_build_object(
            'schema_name', ht.schema_name,
            'name', ht.table_name, 
            'older_than', d.older_than, 
            'cascade_to_materializations', d.cascade_to_materializations) AS json_parameters
   FROM _timescaledb_config.bgw_job job,
        _timescaledb_config.bgw_policy_drop_chunks d,
        _timescaledb_catalog.hypertable ht 
  WHERE job.id = d.job_id and ht.id = d.hypertable_id
UNION
SELECT 
    ht.schema_name,
    ht.table_name,
    job.id,                                  
    job.application_name,
    job.job_type,
    job.schedule_interval,
    job.max_runtime,
    job.max_retries,
    job.retry_period,
    jsonb_build_object(
            'schema_name', ht.schema_name,
            'name', ht.table_name, 
            'hypertable_index_name', hypertable_index_name) AS json_parameters 
   FROM _timescaledb_config.bgw_job job,
        _timescaledb_config.bgw_policy_reorder d,
        _timescaledb_catalog.hypertable ht 
  WHERE job.id = d.job_id and ht.id = d.hypertable_id;



-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE VIEW timescaledb_experimental.chunk_replication_status AS
SELECT
    h.schema_name AS hypertable_schema,
    h.table_name AS hypertable_name,
    c.schema_name AS chunk_schema,
    c.table_name AS chunk_name,
    h.replication_factor AS desired_num_replicas,
    count(cdn.chunk_id) AS num_replicas,
    array_agg(cdn.node_name) AS replica_nodes,
    -- compute the set of data nodes that doesn't have the chunk
    (SELECT array_agg(node_name) FROM
            (SELECT node_name FROM _timescaledb_catalog.hypertable_data_node hdn
             WHERE hdn.hypertable_id = h.id
             EXCEPT
             SELECT node_name FROM _timescaledb_catalog.chunk_data_node cdn
             WHERE cdn.chunk_id = c.id
             ORDER BY node_name) nodes) AS non_replica_nodes
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.chunk_data_node cdn ON (cdn.chunk_id = c.id)
INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = c.hypertable_id)
GROUP BY h.id, c.id, hypertable_schema, hypertable_name, chunk_schema, chunk_name
ORDER BY h.id, c.id, hypertable_schema, hypertable_name, chunk_schema, chunk_name;

CREATE OR REPLACE VIEW timescaledb_experimental.policies AS
SELECT ca.view_name AS relation_name,
  ca.view_schema AS relation_schema,
  j.schedule_interval,
  j.proc_schema,
  j.proc_name,
  j.config,
  ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name
FROM _timescaledb_config.bgw_job j, timescaledb_information.continuous_aggregates ca,
 _timescaledb_catalog.hypertable ht
WHERE ht.id = j.hypertable_id
AND ca.view_schema = hypertable_schema
AND ca.hypertable_name <> ht.table_name;

GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_experimental TO PUBLIC;

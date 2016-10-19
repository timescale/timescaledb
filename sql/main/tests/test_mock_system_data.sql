WITH ns AS (
  INSERT INTO namespaces(name, project_id, partitioning_field) 
  VALUES ('testns', 22, 'field_1') 
  RETURNING *
), storage_records AS (
  INSERT INTO kafka_storage_records(namespace_id, topic, start_offset, end_offset) 
  VALUES ((select ns.namespace_id from ns), '', 0, 0) 
  RETURNING *
), fields AS (
  INSERT INTO namespace_fields VALUES 
   ((select ns.namespace_id from ns), 'DOUBLE', 'field_1'),
   ((select ns.namespace_id from ns), 'STRING', 'field_2'),
   ((select ns.namespace_id from ns), 'DOUBLE', 'field_3'),
   ((select ns.namespace_id from ns), 'STRING', 'Field_4')
)
  INSERT INTO namespace_labels(namespace_id, name, value) VALUES
    ((select ns.namespace_id from ns), 'field_1:distinct', 'TRUE'),
    ((select ns.namespace_id from ns), 'field_2:distinct', 'true'),
    ((select ns.namespace_id from ns), 'field_3:distinct', 'FalSE');
    -- field 4 missing label which should mean distinct == false


WITH ns AS (
  INSERT INTO namespaces(name, project_id, partitioning_field) 
  VALUES ('testns', 23, 'field_1') 
  RETURNING *
), storage_records AS (
  INSERT INTO kafka_storage_records(namespace_id, topic, start_offset, end_offset) 
  VALUES ((select ns.namespace_id from ns), '', 0, 0) 
  RETURNING *
), fields AS (
  INSERT INTO namespace_fields VALUES 
   ((select ns.namespace_id from ns), 'DOUBLE', 'field_1'),
   ((select ns.namespace_id from ns), 'STRING', 'field_2'),
   ((select ns.namespace_id from ns), 'DOUBLE', 'field_3')
)
  INSERT INTO namespace_labels(namespace_id, name, value) VALUES
    ((select ns.namespace_id from ns), 'field_1:distinct', 'TRUE'),
    ((select ns.namespace_id from ns), 'field_2:distinct', 'true'),
    ((select ns.namespace_id from ns), 'field_3:distinct', 'FALSE');



CREATE TABLE IF NOT EXISTS public.project_fields (
		project_id BIGINT,
		namespace TEXT,
		field TEXT,
		cluster_table regclass,
		value_type regtype,
		is_partition_key boolean,
		is_distinct boolean,
		server_name TEXT,
		PRIMARY KEY (project_id, namespace, field)
	);

CREATE OR REPLACE FUNCTION register_project_field(
		project_id BIGINT,
		namespace TEXT,
		field TEXT,
		cluster_table regclass,
		value_type regtype,
		is_partition_key boolean,
		is_distinct boolean
) RETURNS VOID AS $$
	INSERT INTO public.project_fields (SELECT project_id, namespace, field, cluster_table, value_type, is_partition_key, is_distinct, 'local') ON CONFLICT DO NOTHING
$$
LANGUAGE 'sql' VOLATILE;

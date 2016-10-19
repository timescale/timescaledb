create schema if not exists cluster;

CREATE TABLE IF NOT EXISTS cluster.project_field (
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


CREATE TABLE IF NOT EXISTS public.project_field (
		PRIMARY KEY (project_id, namespace, field)
) INHERITS(cluster.project_field);

CREATE OR REPLACE FUNCTION register_project_field(
		project_id BIGINT,
		namespace TEXT,
		field TEXT,
		cluster_table regclass,
		value_type regtype,
		is_partition_key boolean,
		is_distinct boolean
) RETURNS VOID AS $$
	INSERT INTO public.project_field (SELECT project_id, namespace, field, cluster_table, value_type, is_partition_key, is_distinct, 'local') ON CONFLICT DO NOTHING
$$
LANGUAGE 'sql';

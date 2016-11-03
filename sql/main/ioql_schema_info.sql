CREATE OR REPLACE FUNCTION get_partitions_for_namespace(namespace_name NAME)
    RETURNS SETOF namespace_partition_type LANGUAGE SQL STABLE AS
$BODY$
SELECT DISTINCT
    dt.namespace_name,
    dt.partition_number,
    dt.total_partitions
FROM data_table AS dt
WHERE dt.namespace_name = get_partitions_for_namespace.namespace_name
$BODY$;

CREATE OR REPLACE FUNCTION get_data_tables_for_partitions_time_desc(_np namespace_partition_type)
    RETURNS SETOF data_table LANGUAGE SQL STABLE AS
$BODY$
SELECT dt.*
FROM data_table dt
WHERE
    dt.namespace_name = _np.namespace_name AND
    dt.partition_number = _np.partition_number AND
    dt.total_partitions = _np.total_partitions
ORDER BY GREATEST(start_time, end_time) DESC
$BODY$;

CREATE OR REPLACE FUNCTION get_partition_table_row(np namespace_partition_type)
    RETURNS partition_table LANGUAGE SQL STABLE AS
$BODY$
SELECT pt.*
FROM partition_table pt
WHERE
    pt.namespace_name = np.namespace_name AND
    pt.partition_number = np.partition_number AND
    pt.total_partitions = np.total_partitions
$BODY$;
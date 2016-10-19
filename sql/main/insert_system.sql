
CREATE OR REPLACE FUNCTION initialize_field_from_system_connector(
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT,
    field_name       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    pfl project_field_local;
BEGIN
    pfl = field_info_from_system_connector(project_id :: BIGINT, namespace, replica_no, field_name);

    PERFORM create_field_from_definition(pfl.project_id, pfl.namespace, pfl.replica_no, pfl.field, pfl.is_distinct,
                                         pfl.is_partition_key, pfl.value_type, pfl.idx_types);
END
$BODY$;

CREATE OR REPLACE FUNCTION register_field_from_system_connector(
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT,
    field_name       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    pfl project_field_local;
BEGIN
    pfl = field_info_from_system_connector(project_id :: BIGINT, namespace, replica_no, field_name);
    PERFORM register_project_field(
        pfl.project_id, pfl.namespace, pfl.replica_no, pfl.field,
        pfl.value_type, pfl.is_partition_key, pfl.is_distinct, pfl.idx_types);
END
$BODY$;


CREATE OR REPLACE FUNCTION field_info_from_system_connector(
    project_id BIGINT,
    namespace  TEXT,
    replica_no SMALLINT,
    field      TEXT
)
    RETURNS project_field_local LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    field_name            TEXT;
    field_type            TEXT;
    field_type_oid        REGTYPE;
    field_is_distinct     BOOLEAN;
    field_is_partitioning BOOLEAN;
    index_type_string     TEXT;
    index_types           field_index_type [];
BEGIN

    SELECT
        nsf.field,
        nsf.type,
        (lower(nl_distinct.value) IS NOT DISTINCT FROM 'true'),
        ns.partitioning_field = nsf.field,
        upper(nl_idx_type.value) :: field_index_type
    INTO STRICT field_name, field_type, field_is_distinct, field_is_partitioning, index_type_string
    FROM system.namespaces AS ns
    INNER JOIN system.namespace_fields AS nsf ON (ns.namespace_id = nsf.namespace_id)
    LEFT JOIN system.namespace_labels AS nl_distinct
        ON (nl_distinct.namespace_id = ns.namespace_id AND nl_distinct.name = nsf.field || ':distinct')
    LEFT JOIN system.namespace_labels AS nl_idx_type
        ON (nl_idx_type.namespace_id = ns.namespace_id AND nl_idx_type.name = nsf.field || ':index_type')
    WHERE
        ns.project_id = field_info_from_system_connector.project_id AND
        ns.name = field_info_from_system_connector.namespace AND
        nsf.field = field_info_from_system_connector.field;

    SELECT t :: REGTYPE
    INTO field_type_oid
    FROM get_field_type_from_ns_type(field_type) AS t;

    index_types = string_to_array(index_type_string, ',') :: field_index_type [];

    IF index_types IS NULL
    THEN
        IF field_is_distinct OR field_type_oid :: REGTYPE = 'TEXT' :: REGTYPE THEN
            index_types = '{VALUE-TIME}'; --'{VALUE-TIME,TIME-VALUE}'
        ELSE
            index_types = '{TIME-VALUE}';
        END IF;
    END IF;

    RETURN ROW (project_id, namespace, replica_no, field_name, field_type_oid, field_is_partitioning,
           field_is_distinct, index_types, NULL :: TEXT);
END
$BODY$;
-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- main_table - The OID of the table to be converted
-- time_field_name - Name of the field that contains time for a given record
-- partitioning_field - Name of the field to partition data by
-- replication_factor -- (Optional) Number of replicas for data
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
-- hypertable_name - (Optional) Name for the hypertable, if different than the main table name
CREATE OR REPLACE FUNCTION  add_hypertable(
    main_table              REGCLASS,
    time_field_name         NAME,
    partitioning_field      NAME,
    replication_factor      SMALLINT = 1,
    number_partitions       SMALLINT = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    hypertable_name         NAME = NULL,
    placement               chunk_placement_type = 'STICKY'
)
    RETURNS hypertable LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
  hypertable_row hypertable;
  table_name NAME;
  schema_name NAME;
  time_field_type REGTYPE;
  att_row pg_attribute;
BEGIN
       SELECT relname, nspname
       INTO STRICT table_name, schema_name
       FROM pg_class c
       INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
       WHERE c.OID = main_table;

       SELECT atttypid
       INTO STRICT time_field_type
       FROM pg_attribute
       WHERE attrelid = main_table AND attname = time_field_name;
       PERFORM dblink_connect('meta_conn', get_meta_server_name());
       PERFORM dblink_exec('meta_conn', 'BEGIN');

        SELECT (t.r::hypertable).*
        INTO hypertable_row
        FROM dblink(
          'meta_conn',
          format('SELECT t FROM _meta.add_hypertable(%L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L) t ',
            schema_name,
            table_name,
            time_field_name,
            time_field_type,
            partitioning_field,
            replication_factor,
            number_partitions,
            associated_schema_name,
            associated_table_prefix,
            hypertable_name,
            placement,
            current_database()
        )) AS t(r TEXT);

      FOR att_row IN SELECT *
       FROM pg_attribute att
       WHERE attrelid = main_table AND attnum > 0 AND NOT attisdropped
      LOOP
        PERFORM  _sysinternal.create_column_from_attribute(hypertable_row.name, att_row, 'meta_conn');
      END LOOP;


      PERFORM 1
      FROM pg_index,
      LATERAL dblink(
          'meta_conn',
          format('SELECT _meta.add_index(%L, %L,%L, %L, %L)',
            hypertable_row.name,
            hypertable_row.main_schema_name,
            (SELECT relname FROM pg_class WHERE oid = indexrelid::regclass),
            _sysinternal.get_general_index_definition(indexrelid, indrelid),
            current_database()
        )) AS t(r TEXT)
      WHERE indrelid = main_table;

      PERFORM dblink_exec('meta_conn', 'COMMIT');
      PERFORM dblink_disconnect('meta_conn');
      RETURN hypertable_row;
END
$BODY$;

-- Sets the is_distinct flag for field on a hypertable.
-- The is_distinct flag determines whether the system keep a materialized list
-- of distinct values for the field.
CREATE OR REPLACE FUNCTION set_is_distinct_flag(
    main_table    REGCLASS,
    field_name    NAME,
    is_distinct   BOOLEAN

)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
  table_name NAME;
  schema_name NAME;
  hypertable_row hypertable;
BEGIN
    SELECT relname, nspname
    INTO STRICT table_name, schema_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    SELECT * INTO hypertable_row
    FROM hypertable h
    WHERE main_schema_name = schema_name AND
          main_table_name = table_name;

    PERFORM *
    FROM dblink(
      get_meta_server_name(),
      format('SELECT _meta.alter_column_set_is_distinct(%L, %L, %L, %L)',
        hypertable_row.name,
        field_name,
        is_distinct,
        current_database()
    )) AS t(r TEXT);
END
$BODY$;

-- This file contains functions related to getting information about the
-- schema of a hypertable, including columns, their types, etc.

-- Get the name of the time column for a hypertable.
CREATE OR REPLACE FUNCTION get_time_column(
    hypertable_id INTEGER
)
    RETURNS NAME LANGUAGE SQL STABLE AS
$BODY$
    SELECT time_column_name
    FROM _iobeamdb_catalog.hypertable h
    WHERE h.id = hypertable_id;
$BODY$;

-- Get the type of the time column for a hypertable.
CREATE OR REPLACE FUNCTION get_time_column_type(
    hypertable_id INTEGER
)
    RETURNS REGTYPE LANGUAGE SQL STABLE AS
$BODY$
    SELECT time_column_type
    FROM _iobeamdb_catalog.hypertable h
    WHERE h.id = hypertable_id;
$BODY$;

-- Get the list of columns (each quoted) from a hypertable as an ARRAY
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_quoted_column_names(
    hypertable_id INTEGER
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
    SELECT ARRAY(
        SELECT format('%I', name)
        FROM _iobeamdb_catalog.hypertable_column c
        WHERE c.hypertable_id = get_quoted_column_names.hypertable_id
        ORDER BY name
    );
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_partition_for_epoch(
    epoch     _iobeamdb_catalog.partition_epoch,
    key_value TEXT
)
    RETURNS _iobeamdb_catalog.partition LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
    partition_row _iobeamdb_catalog.partition;
BEGIN
    IF epoch.partitioning_column IS NULL THEN
        SELECT p.*
        FROM  _iobeamdb_catalog.partition p
        WHERE p.epoch_id = epoch.id
        INTO STRICT partition_row;
    ELSE
        EXECUTE format(
            $$
                SELECT p.*
                FROM  _iobeamdb_catalog.partition p
                WHERE p.epoch_id = %L AND
                %I.%I(%L::TEXT, %L) BETWEEN p.keyspace_start AND p.keyspace_end
            $$, epoch.id, epoch.partitioning_func_schema, epoch.partitioning_func, key_value, epoch.partitioning_mod)
        INTO STRICT partition_row;
    END IF;
    RETURN partition_row;
END
$BODY$;

-- TODO Only used in tests -- okay to put in _iobeamdb_internal?
CREATE OR REPLACE FUNCTION get_open_partition_for_key(
    hypertable_id INTEGER,
    key_value     TEXT
)
    RETURNS _iobeamdb_catalog.partition LANGUAGE SQL STABLE AS
$BODY$
    SELECT p.*
    FROM _iobeamdb_catalog.partition_epoch pe,
         _iobeamdb_internal.get_partition_for_epoch(pe, key_value) p
    WHERE pe.hypertable_id = get_open_partition_for_key.hypertable_id AND
          end_time IS NULL
$BODY$;

-- Check if a given table OID is a main table (i.e. the table a user
-- targets for SQL operations) for a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_internal.is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
    SELECT EXISTS(SELECT 1 FROM _iobeamdb_catalog.hypertable WHERE table_name = relname AND schema_name = nspname)
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = table_oid;
$BODY$;


-- Get a hypertable given its main table OID
CREATE OR REPLACE FUNCTION _iobeamdb_internal.hypertable_from_main_table(
    table_oid regclass
)
    RETURNS _iobeamdb_catalog.hypertable LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.*
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INNER JOIN _iobeamdb_catalog.hypertable h ON (h.table_name = c.relname AND h.schema_name = n.nspname)
    WHERE c.OID = table_oid;
$BODY$;

-- Get the name of the time column for a chunk_replica_node.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.time_col_name_for_crn(
    schema_name NAME,
    table_name  NAME
)
    RETURNS NAME LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_name NAME;
BEGIN
    SELECT h.time_column_name INTO STRICT time_col_name
    FROM _iobeamdb_catalog.hypertable h
    INNER JOIN _iobeamdb_catalog.partition_epoch pe ON (pe.hypertable_id = h.id)
    INNER JOIN _iobeamdb_catalog.partition p ON (p.epoch_id = pe.id)
    INNER JOIN _iobeamdb_catalog.chunk c ON (c.partition_id = p.id)
    INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (crn.chunk_id = c.id)
    WHERE crn.schema_name = time_col_name_for_crn.schema_name AND
    crn.table_name = time_col_name_for_crn.table_name;
    RETURN time_col_name;
END
$BODY$;

-- Get the type of the time column for a chunk_replica_node.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.time_col_type_for_crn(
    schema_name NAME,
    table_name  NAME
)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_type REGTYPE;
BEGIN
    SELECT h.time_column_type INTO STRICT time_col_type
    FROM _iobeamdb_catalog.hypertable h
    INNER JOIN _iobeamdb_catalog.partition_epoch pe ON (pe.hypertable_id = h.id)
    INNER JOIN _iobeamdb_catalog.partition p ON (p.epoch_id = pe.id)
    INNER JOIN _iobeamdb_catalog.chunk c ON (c.partition_id = p.id)
    INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (crn.chunk_id = c.id)
    WHERE crn.schema_name = time_col_type_for_crn.schema_name AND
    crn.table_name = time_col_type_for_crn.table_name;
    RETURN time_col_type;
END
$BODY$;

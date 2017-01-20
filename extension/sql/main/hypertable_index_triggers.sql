/*
    Convert a general index definition to a create index sql command
    for a particular table and index name.
 */
CREATE OR REPLACE FUNCTION _sysinternal.get_index_definition_for_table(
    schema_name NAME,
    table_name  NAME,
    index_name NAME,
    general_defintion TEXT
  )
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code TEXT;
BEGIN
    sql_code := replace(general_defintion, '/*TABLE_NAME*/', format('%I.%I', schema_name, table_name));
    sql_code = replace(sql_code, '/*INDEX_NAME*/', format('%I', index_name));

    RETURN sql_code;
END
$BODY$;

/*
    Creates an index on all chunk_replica_nodes for a hypertable.
*/
CREATE OR REPLACE FUNCTION _sysinternal.create_index_on_all_chunk_replica_nodes(
    hypertable_name  NAME,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _sysinternal.create_chunk_replica_node_index(crn.schema_name, crn.table_name, main_schema_name, main_index_name, definition)
    FROM _iobeamdb_catalog.chunk_replica_node crn
    INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
    WHERE pr.hypertable_name = create_index_on_all_chunk_replica_nodes.hypertable_name AND
          crn.database_name = current_database();
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.drop_chunk_replica_node_index(
    main_schema_name     NAME,
    main_index_name      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    DELETE FROM _iobeamdb_catalog.chunk_replica_node_index crni
    WHERE crni.main_index_name = drop_chunk_replica_node_index.main_index_name AND
 crni.main_schema_name = drop_chunk_replica_node_index.main_schema_name
$BODY$;

/*
    Creates indexes on chunk tables when hypertable_index rows created.
*/
CREATE OR REPLACE FUNCTION _sysinternal.on_modify_hypertable_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  hypertable_row _iobeamdb_catalog.hypertable;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'Only inserts/deletes supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    IF TG_OP = 'DELETE' THEN
      RETURN OLD; --handled by deleted_hypertable_index table
    END IF;

    --create index on all chunks
    PERFORM _sysinternal.create_index_on_all_chunk_replica_nodes(NEW.hypertable_name, NEW.main_schema_name, NEW.main_index_name, NEW.definition);

    IF new.created_on <> current_database() THEN
      --create index on main table
      SELECT *
      INTO STRICT hypertable_row
      FROM _iobeamdb_catalog.hypertable AS h
      WHERE h.name = NEW.hypertable_name;

      PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
      EXECUTE _sysinternal.get_index_definition_for_table(hypertable_row.main_schema_name, hypertable_row.main_table_name, NEW.main_index_name, NEW.definition);
    END IF;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';


/*
    Drops indexes on chunk tables when hypertable_index rows deleted (row created in deleted_hypertable_index table).
*/
CREATE OR REPLACE FUNCTION _sysinternal.on_deleted_hypertable_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row _iobeamdb_catalog.hypertable;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;
    --drop index on all chunks
    PERFORM _sysinternal.drop_chunk_replica_node_index(NEW.main_schema_name, NEW.main_index_name);

    IF new.deleted_on <> current_database() THEN
      PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
      --note: index might have been deleted by field deletion ahead of time. IF EXISTS necessary
      EXECUTE format('DROP INDEX IF EXISTS %I.%I', NEW.main_schema_name, NEW.main_index_name);
    END IF;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

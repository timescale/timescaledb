/*
    Convert a general index definition to a create index sql command
    for a particular table and index name.
 */
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_index_definition_for_table(
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
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_index_on_all_chunk_replica_nodes(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.create_chunk_replica_node_index(crn.schema_name, crn.table_name, main_schema_name, main_index_name, definition)
    FROM _timescaledb_catalog.chunk_replica_node crn
    INNER JOIN _timescaledb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
    WHERE pr.hypertable_id = create_index_on_all_chunk_replica_nodes.hypertable_id AND
          crn.database_name = current_database();
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_replica_node_index(
    main_schema_name     NAME,
    main_index_name      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    DELETE FROM _timescaledb_catalog.chunk_replica_node_index crni
    WHERE crni.main_index_name = drop_chunk_replica_node_index.main_index_name AND
 crni.main_schema_name = drop_chunk_replica_node_index.main_schema_name
$BODY$;

/*
    Creates indexes on chunk tables when hypertable_index rows created.
*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_hypertable_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        --create index on all chunks
        PERFORM _timescaledb_internal.create_index_on_all_chunk_replica_nodes(NEW.hypertable_id, NEW.main_schema_name, NEW.main_index_name, NEW.definition);

        IF current_setting('timescaledb_internal.originating_node') <> 'on' THEN
          --create index on main table
          SELECT *
          INTO STRICT hypertable_row
          FROM _timescaledb_catalog.hypertable AS h
          WHERE h.id = NEW.hypertable_id;

          PERFORM set_config('timescaledb_internal.ignore_ddl', 'true', true);
          EXECUTE _timescaledb_internal.get_index_definition_for_table(hypertable_row.schema_name, hypertable_row.table_name, NEW.main_index_name, NEW.definition);
        END IF;

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM _timescaledb_internal.drop_chunk_replica_node_index(OLD.main_schema_name, OLD.main_index_name);
        
        IF current_setting('timescaledb_internal.originating_node') <> 'on' THEN
            PERFORM set_config('timescaledb_internal.ignore_ddl', 'true', true);
            --note: index might have been deleted by field deletion ahead of time. IF EXISTS necessary
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', OLD.main_schema_name, OLD.main_index_name);
        END IF;
        RETURN OLD;
    END IF;
END
$BODY$;



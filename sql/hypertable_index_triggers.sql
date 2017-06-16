-- Creates an index on all chunk for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_index_on_all_chunks(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.create_chunk_index_row(c.schema_name, c.table_name, main_schema_name, main_index_name, definition)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = create_index_on_all_chunks.hypertable_id;
END
$BODY$;

-- Drops table on all chunks for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_index_on_all_chunks(
    main_schema_name     NAME,
    main_index_name      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    DELETE FROM _timescaledb_catalog.chunk_index ci
    WHERE ci.main_index_name = drop_index_on_all_chunks.main_index_name AND
    ci.main_schema_name = drop_index_on_all_chunks.main_schema_name
$BODY$;


-- Creates indexes on chunk tables when hypertable_index rows created.
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
        -- create index on all chunks
        PERFORM _timescaledb_internal.create_index_on_all_chunks(NEW.hypertable_id, NEW.main_schema_name, NEW.main_index_name, NEW.definition);

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM _timescaledb_internal.drop_index_on_all_chunks(OLD.main_schema_name, OLD.main_index_name);

        RETURN OLD;
    END IF;
END
$BODY$;



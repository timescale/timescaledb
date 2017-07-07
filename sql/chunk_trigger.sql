-- Convert a general trigger definition to a create trigger sql command for a
-- particular table and trigger name.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_trigger_definition_for_table(
    chunk_id INTEGER,
    general_definition TEXT
  )
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    sql_code TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk WHERE id = chunk_id;
    sql_code := replace(general_definition, '/*TABLE_NAME*/', format('%I.%I', chunk_row.schema_name, chunk_row.table_name));
    RETURN sql_code;
END
$BODY$;

-- Creates a chunk_trigger_row.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_trigger(
    chunk_id INTEGER,
    trigger_name NAME,
    def TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
BEGIN
    sql_code := _timescaledb_internal.get_trigger_definition_for_table(chunk_id, def);
    EXECUTE sql_code;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_trigger(
    chunk_id INTEGER,
    trigger_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk WHERE id = chunk_id;
    EXECUTE format($$ DROP TRIGGER IF EXISTS %I ON %I.%I $$, trigger_name, chunk_row.schema_name, chunk_row.table_name);
END
$BODY$
SET client_min_messages = WARNING;

-- Creates a trigger on all chunk for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_trigger_on_all_chunks(
    hypertable_id INTEGER,
    trigger_name     NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.create_chunk_trigger(c.id, trigger_name, definition)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = create_trigger_on_all_chunks.hypertable_id;
END
$BODY$;

-- Drops trigger on all chunks for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_trigger_on_all_chunks(
    hypertable_id INTEGER,
    trigger_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.drop_chunk_trigger(c.id, trigger_name)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = drop_trigger_on_all_chunks.hypertable_id;
END
$BODY$;

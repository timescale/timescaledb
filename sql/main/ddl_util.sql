/*
  Create the "general definition" of an index. The general definition
  is the corresponding create index command with the placeholders /*TABLE_NAME*/
  and /*INDEX_NAME*/
*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_general_index_definition(
    index_oid REGCLASS,
    table_oid REGCLASS)
RETURNS text
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    def             TEXT;
    c               INTEGER;
    v_index_name    TEXT;
    v_table_name    TEXT;
BEGIN
    -- Get index definition text
    def := pg_get_indexdef(index_oid);

    IF def IS NULL THEN
        RAISE EXCEPTION 'Cannot process index with definition (no index name matched) oid %', index_oid;
    END IF;

    -- Get table name for table_oid
    -- Only schema qualify the table if the schema is not "public"
    SELECT (CASE WHEN n.nspname IS NOT NULL AND n.nspname <> 'public' THEN n.nspname||'.' ELSE '' END)||c.relname
        INTO STRICT v_table_name
    FROM pg_catalog.pg_class AS c
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    -- Replace table name in index def with /*TABLE_NAME*/
    IF v_table_name IS NULL THEN
        RAISE EXCEPTION 'Cannot process index with definition (no table name matched) %', def;
    END IF;
    def := replace(def,  'ON '|| v_table_name || ' USING', 'ON /*TABLE_NAME*/ USING');

    -- Get index name for index_oid
    SELECT c.relname
        INTO STRICT v_index_name
    FROM pg_catalog.pg_class AS c
    WHERE c.oid = index_oid
    AND c.relkind = 'i'::CHAR;

    -- Replace index name with /*INDEX_NAME*/
    -- Index name is never schema qualified
    def := replace(def, 'INDEX '|| format('%I', v_index_name) || ' ON', 'INDEX /*INDEX_NAME*/ ON');

    RETURN def;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_default_value_for_attribute(
    att pg_attribute
)
    RETURNS TEXT LANGUAGE SQL VOLATILE AS
$BODY$
    SELECT adsrc
    FROM pg_attrdef def
    WHERE def.adrelid = att.attrelid AND def.adnum = att.attnum
$BODY$;

--create the hypertable column based on a pg_attribute (table column) entry.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_column_from_attribute(
    hypertable_id INTEGER,
    att           pg_attribute
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _timescaledb_meta_api.add_column(
        hypertable_id,
        att.attname,
        att.attnum,
        att.atttypid,
        _timescaledb_internal.get_default_value_for_attribute(att),
        att.attnotnull
    );
END
$BODY$;

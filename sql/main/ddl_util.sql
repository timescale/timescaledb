/*
  Create the "general definition" of an index. The general definition
  is the corresponding create index command with the placeholders /*TABLE_NAME*/
  and  /*INDEX_NAME*/
*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_general_index_definition(
    index_oid regclass,
    table_oid regclass)
RETURNS text
LANGUAGE plpgsql VOLATILE AS
$BODY$
DECLARE
    def TEXT;
BEGIN
    -- Get index definition
    def := pg_get_indexdef(index_oid);

    IF def IS NULL THEN
        RAISE EXCEPTION 'Cannot process index with definition (no index name matched: %)', index_oid::TEXT;
    END IF;

    def := replace(def, 'ON '|| table_oid::TEXT || ' USING', 'ON /*TABLE_NAME*/ USING');

    -- Replace index name with /*INDEX_NAME*/
    -- Index name is never schema qualified
    -- Mixed case identifiers are properly handled.
    SELECT replace(
            def,
            'INDEX '|| format('%I', (SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = index_oid AND c.relkind = 'i'::CHAR) ) || ' ON',
            'INDEX /*INDEX_NAME*/ ON')
    INTO def;

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

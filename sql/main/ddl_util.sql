/*
  Create the "general definition" of an index. The general definition
  is the corresponding create index command with the placeholders /*TABLE_NAME*/
  and  /*INDEX_NAME*/
*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_general_index_definition(
    index_oid regclass,
    table_oid regclass
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    def TEXT;
    c   INTEGER;
    index_name TEXT;
BEGIN
    --get definition text
    def = pg_get_indexdef(index_oid);

    --replace table name in def with /*TABLE_NAME*/
    SELECT count(*) INTO c
    FROM regexp_matches(def, 'ON '||table_oid::TEXT || ' USING', 'g');
    IF c <> 1 THEN
        RAISE EXCEPTION 'Cannot process index with definition(not table name match) %', def;
    END IF;
    def = replace(def,  'ON '||table_oid::TEXT || ' USING', 'ON /*TABLE_NAME*/ USING');

    --replace index name with /*INDEX_NAME*/

    SELECT relname
    INTO STRICT index_name --index name in def is never schema qualified
    FROM pg_class
    WHERE OID = index_oid;

    SELECT count(*) INTO c
    FROM regexp_matches(def, 'CREATE INDEX '||  format('%I', index_name) || ' ON', 'g');
    IF c <> 1 THEN
        RAISE EXCEPTION 'Cannot process index with definition (no index name match) %', def;
    END IF;
    def = replace(def, 'CREATE INDEX '|| format('%I', index_name) || ' ON', 'CREATE INDEX /*INDEX_NAME*/ ON');

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

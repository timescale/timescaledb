/*
  Create the "general definition" of an index. The general definition
  is the corresponding create index command with the placeholders /*TABLE_NAME*/
  and  /*INDEX_NAME*/
*/
CREATE OR REPLACE FUNCTION _sysinternal.get_general_index_definition(
  index_oid regclass,
  table_oid regclass
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
  def TEXT;
  c INTEGER;
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
    SELECT count(*) INTO c
    FROM regexp_matches(def, 'CREATE INDEX '||  index_oid || ' ON', 'g');
    IF c <> 1 THEN
      RAISE EXCEPTION 'Cannot process index with definition (no index name match) %', def;
    END IF;
    def = replace(def, 'CREATE INDEX '||  index_oid || ' ON', 'CREATE INDEX /*INDEX_NAME*/ ON');

    RETURN def;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.get_default_value_for_attribute(
  att pg_attribute
)
    RETURNS TEXT LANGUAGE SQL VOLATILE AS
$BODY$
  SELECT adsrc
  FROM pg_attrdef def
  WHERE def.adrelid = att.attrelid AND def.adnum = att.attnum
$BODY$;

--create the hypertable column based on a pg_attribute (table column) entry.
CREATE OR REPLACE FUNCTION _sysinternal.create_column_from_attribute(
  hypertable_name NAME,
  att pg_attribute
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
  PERFORM
    _iobeamdb_meta_api.add_column(
            hypertable_name,
            att.attname,
            att.attnum,
            att.atttypid,
            _sysinternal.get_default_value_for_attribute(att),
            att.attnotnull,
            FALSE
    );
END
$BODY$;

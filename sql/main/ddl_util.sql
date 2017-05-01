/*
  Create the "general definition" of an index. The general definition
  is the corresponding create index command with the placeholders /*TABLE_NAME*/
  and  /*INDEX_NAME*/
*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_general_index_definition(
    index_oid       REGCLASS,
    table_oid       REGCLASS,
    hypertable_row  _timescaledb_catalog.hypertable
)
RETURNS text
LANGUAGE plpgsql VOLATILE AS
$BODY$
DECLARE
    def             TEXT;
    index_name      TEXT;
    c               INTEGER;
    index_row       RECORD;
    missing_column  TEXT;
BEGIN
    -- Get index definition
    def := pg_get_indexdef(index_oid);

    IF def IS NULL THEN
        RAISE EXCEPTION 'Cannot process index with no definition: %', index_oid::TEXT;
    END IF;

    SELECT * INTO STRICT index_row FROM pg_index WHERE indexrelid = index_oid;

    IF index_row.indisunique THEN
        --unique index must contain time and all partition columns from all epochs
        SELECT count(*) INTO c
        FROM pg_attribute
        WHERE attrelid = table_oid AND
              attnum = ANY(index_row.indkey) AND
              attname = hypertable_row.time_column_name;

        IF c < 1 THEN
            RAISE EXCEPTION 'Cannot create a unique index without the % column', hypertable_row.time_column_name
            USING ERRCODE = 'IO103';
        END IF;

        --get any partitioning columns that are not included in the index.
        SELECT partitioning_column INTO missing_column
        FROM _timescaledb_catalog.partition_epoch
        WHERE hypertable_id = hypertable_row.id AND
              partitioning_column NOT IN (
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = table_oid AND
                attnum = ANY(index_row.indkey)
            );

        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'Cannot create a unique index without the partitioning column: %', missing_column
            USING ERRCODE = 'IO103';
        END IF;
    END IF;


    SELECT count(*) INTO c
    FROM regexp_matches(def, 'ON '||table_oid::TEXT || ' USING', 'g');
    IF c <> 1 THEN
         RAISE EXCEPTION 'Cannot process index with definition(no table name match): %', def
         USING ERRCODE = 'IO103';
    END IF;

    def := replace(def, 'ON '|| table_oid::TEXT || ' USING', 'ON /*TABLE_NAME*/ USING');

    -- Replace index name with /*INDEX_NAME*/
    -- Index name is never schema qualified
    -- Mixed case identifiers are properly handled.
    SELECT format('%I', c.relname) INTO STRICT index_name FROM pg_catalog.pg_class AS c WHERE c.oid = index_oid AND c.relkind = 'i'::CHAR;

    SELECT count(*) INTO c
    FROM regexp_matches(def, 'INDEX '|| index_name || ' ON', 'g');
    IF c <> 1 THEN
         RAISE EXCEPTION 'Cannot process index with definition(no index name match): %', def
         USING ERRCODE = 'IO103';
    END IF;

    def := replace(def, 'INDEX '|| index_name || ' ON',  'INDEX /*INDEX_NAME*/ ON');

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

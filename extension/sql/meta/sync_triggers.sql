CREATE OR REPLACE FUNCTION _sysinternal.sync_insert()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.%I SELECT $1.*
            $$,
            schema_name,
            TG_TABLE_NAME
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.sync_update()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
    primary_key_sql TEXT;
    update_col_sql TEXT; 
BEGIN
    --primary key sql goes into the where clause 
    SELECT string_agg(format('%1$I = $1.%1$I', a.attname), ' AND ') INTO primary_key_sql
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = TG_RELID::regclass
    AND    i.indisprimary AND attnum > 0;

    --update_col_sql goes into the set clause
    SELECT string_agg(format('%1$I = $2.%1$I', a.attname), ', ') INTO update_col_sql
    FROM   pg_attribute a 
    WHERE  a.attrelid = TG_RELID::regclass
    AND    a.attnum > 0;
    
    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    LOOP
        EXECUTE format(
            $$
                UPDATE %I.%I SET  %s WHERE %s
            $$,
            schema_name,
            TG_TABLE_NAME,
            update_col_sql,
            primary_key_sql
        )
        USING OLD, NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.sync_delete()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
    primary_key_sql TEXT;
BEGIN
    --primary key sql goes into the where clause 
    SELECT string_agg(format('%1$I = $1.%1$I', a.attname), ' AND ') INTO primary_key_sql
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = TG_RELID::regclass
    AND    i.indisprimary AND attnum > 0;

    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    LOOP
        EXECUTE format(
            $$
                DELETE FROM %I.%I WHERE %s
            $$,
            schema_name,
            TG_TABLE_NAME,
            primary_key_sql
        )
        USING OLD;
    END LOOP;
    RETURN OLD;
END
$BODY$;




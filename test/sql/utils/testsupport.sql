CREATE SCHEMA IF NOT EXISTS test;
GRANT USAGE ON SCHEMA test TO PUBLIC;

-- Utility functions to show relation information in tests. These
-- functions generate output which is the same across PostgreSQL
-- versions. Their usage is preferred over psql's '\d <relation>',
-- since that output typically changes across PostgreSQL versions.
CREATE OR REPLACE FUNCTION test.show_columns(rel regclass)
RETURNS TABLE("Column" name,
              "Type" text,
              "Nullable" boolean) LANGUAGE SQL STABLE AS
$BODY$
    SELECT a.attname,
    format_type(t.oid, t.typtypmod),
    a.attnotnull
    FROM pg_attribute a, pg_type t
    WHERE a.attrelid = rel
    AND a.atttypid = t.oid
    AND a.attnum >= 0
    ORDER BY a.attnum;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_columnsp(pattern text)
RETURNS TABLE("Relation" regclass,
              "Kind" "char",
              "Column" name,
              "Column type" text,
              "Nullable" boolean) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT c.oid::regclass,
    c.relkind,
    a.attname,
    format_type(t.oid, t.typtypmod),
    a.attnotnull
    FROM pg_class c, pg_attribute a, pg_type t
    WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
    AND a.attrelid = c.oid
    AND a.atttypid = t.oid
    AND a.attnum >= 0
    ORDER BY c.relname, a.attnum;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_indexes(rel regclass)
RETURNS TABLE("Index" regclass,
              "Columns" name[],
              "Unique" boolean,
              "Primary" boolean,
              "Exclusion" boolean,
              "Tablespace" name) LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.oid::regclass,
    array(SELECT "Column" FROM test.show_columns(c.oid)),
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    (SELECT t.spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)
    FROM pg_class c, pg_index i
    WHERE c.oid = i.indexrelid AND i.indrelid = rel
    ORDER BY c.oid;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_indexesp(pattern text)
RETURNS TABLE("Table" regclass,
              "Index" regclass,
              "Columns" name[],
              "Unique" boolean,
              "Primary" boolean,
              "Exclusion" boolean,
              "Tablespace" name) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT c.oid::regclass,
    i.indexrelid::regclass,
    array(SELECT "Column" FROM test.show_columns(c.oid)),
    i.indisunique,
    i.indisprimary,
    i.indisexclusion,
    (SELECT t.spcname FROM pg_class cc, pg_tablespace t WHERE cc.oid = i.indexrelid AND t.oid = cc.reltablespace)
    FROM pg_class c, pg_index i
    WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
    AND c.oid = i.indrelid
    ORDER BY c.oid, i.indexrelid;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_constraints(rel regclass)
RETURNS TABLE("Constraint" name,
              "Type" "char",
              "Columns" name[],
              "Index" regclass,
              "Expr" text) LANGUAGE SQL STABLE AS
$BODY$
    SELECT c.conname,
    c.contype,
    array(SELECT attname FROM pg_attribute a, unnest(conkey) k WHERE a.attrelid = rel AND k = a.attnum),
    c.conindid::regclass,
    c.consrc
    FROM pg_constraint c
    WHERE c.conrelid = rel
    ORDER BY c.conname;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_constraintsp(pattern text)
RETURNS TABLE("Table" regclass,
              "Constraint" name,
              "Type" "char",
              "Columns" name[],
              "Index" regclass,
              "Expr" text) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT cl.oid::regclass,
    c.conname,
    c.contype,
    array(SELECT attname FROM pg_attribute a, unnest(conkey) k WHERE a.attrelid = cl.oid AND k = a.attnum),
    c.conindid::regclass,
    c.consrc
    FROM pg_class cl, pg_constraint c
    WHERE format('%I.%I', cl.relnamespace::regnamespace::name, cl.relname) LIKE format('%I.%s', schema_name, table_name)
    AND c.conrelid = cl.oid
    ORDER BY cl.relname, c.conname;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_triggers(rel regclass)
RETURNS TABLE("Trigger" name,
              "Type" smallint,
              "Function" regproc,
              "Definition" text) LANGUAGE SQL STABLE AS
$BODY$
    SELECT t.tgname,
    t.tgtype,
    t.tgfoid::regproc,
    -- remove 'CREATE TRIGGER' part from definition
    substring(pg_get_triggerdef(t.oid) from 15)
    FROM pg_trigger t
    WHERE t.tgrelid = rel
    ORDER BY t.tgname;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_triggersp(pattern text)
RETURNS TABLE("Table" regclass,
              "Trigger" name,
              "Type" smallint,
              "Function" regproc,
              "Definition" text) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT t.tgrelid::regclass,
    t.tgname,
    t.tgtype,
    t.tgfoid::regproc,
    -- remove 'CREATE TRIGGER' part from definition
    substring(pg_get_triggerdef(t.oid) from 15)
    FROM pg_class cl, pg_trigger t
    WHERE format('%I.%I', cl.relnamespace::regnamespace::name, cl.relname) LIKE format('%I.%s', schema_name, table_name)
    AND t.tgrelid = cl.oid
    ORDER BY t.tgrelid, t.tgname;
END
$BODY$;

CREATE OR REPLACE FUNCTION test.show_subtables(rel regclass)
RETURNS TABLE("Child" regclass,
              "Tablespace" name) LANGUAGE SQL STABLE AS
$BODY$
    SELECT objid::regclass, (SELECT t.spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace)
    FROM pg_depend d, pg_class c
    WHERE d.refobjid = rel
    AND d.deptype = 'n'
    AND d.classid = 'pg_class'::regclass
    AND d.objid = c.oid
    ORDER BY d.refobjid, d.objid;
$BODY$;

CREATE OR REPLACE FUNCTION test.show_subtablesp(pattern text)
RETURNS TABLE("Parent" regclass,
              "Child" regclass,
              "Tablespace" name) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    schema_name name = split_part(pattern, '.', 1);
    table_name name = split_part(pattern, '.', 2);
BEGIN
    IF schema_name = '' OR table_name = '' THEN
       schema_name := current_schema();
       table_name := pattern;
    END IF;

    RETURN QUERY
    SELECT refobjid::regclass,
    objid::regclass,
    (SELECT t.spcname FROM pg_class cc, pg_tablespace t WHERE cc.oid = d.objid AND t.oid = cc.reltablespace)
    FROM pg_class c, pg_depend d
    WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
    AND d.refobjid = c.oid
    AND d.deptype = 'n'
    AND d.classid = 'pg_class'::regclass
    ORDER BY d.refobjid, d.objid;
END
$BODY$;

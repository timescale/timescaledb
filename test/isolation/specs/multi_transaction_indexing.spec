
setup {
    CREATE TABLE ts_index_test(time int, temp float, location int);
    SELECT create_hypertable('ts_index_test', 'time', chunk_time_interval => 10, create_default_indexes => false);
    INSERT INTO ts_index_test VALUES (1, 23.4, 1),
        (11, 21.3, 2),
        (21, 19.5, 3);

    CREATE TABLE barrier(i INTEGER);
}

teardown {
    DROP TABLE ts_index_test;
    DROP TABLE barrier;
}

session "CREATE INDEX"
step "F" { SET client_min_messages TO 'error'; }
step "CI"	{ CREATE INDEX test_index ON ts_index_test(location) WITH (timescaledb.transaction_per_chunk, timescaledb.barrier_table='barrier'); }

session "RELEASE BARRIER"
setup		{ BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; LOCK TABLE barrier;}
step "Bc"   { ROLLBACK; }

session "SELECT"
setup		{ BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "S1"	{ SELECT * FROM ts_index_test; }
step "Sc"	{ COMMIT; }

session "INSERT CHUNK"
setup		{ BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "I1"	{ INSERT INTO ts_index_test VALUES (31, 6.4, 1); }
step "Ic"	{ COMMIT; }

session "DROP INDEX"
step "DI"	{ DROP INDEX test_index; }

session "RENAME COLUMN"
step "RI"	{ ALTER TABLE test_index RENAME COLUMN location TO height; }

session "COUNT INDEXES"
step "P"    {  SELECT index_size FROM timescaledb_information.hypertable; }

# we need to COMMIT every transaction started in setup regardless of whether we use them
# inserts work between chunks
permutation "CI" "I1" "Ic" "Bc" "P" "Sc"

# create blocks on insert
permutation "I1" "CI" "Bc" "Ic" "P" "Sc"

# create blocks on select
permutation "S1" "CI" "Bc" "Sc" "P" "Ic"

# drop works (the error message outputs an OID, remove the "F" to see the error)
permutation "F" "CI" "DI" "Bc" "P" "Ic" "Sc"

# rename should block
permutation "CI" "RI" "Bc" "P" "Ic" "Sc"

# Ideally we would declare these functions in setup, and use them to check that the actual index
# exist on the relevant chunks in these tests. Unfortunately, in older versions of postgres
# (IIRC until 10.4) there was an arbitrary limit that each SQL statement in an isolation test could
# be no longer than 1024 characters. Instead, we currently use the number of bytes indexs take
# as a proxy. Once we deprecate the old version, or add some other way to get index info we should
# switch to this.
#
#   -- functions from testsupport.sql duplicated here becasue we cannot include sql files
#    CREATE OR REPLACE FUNCTION show_columns(rel regclass)
#    RETURNS TABLE("Column" name,
#                "Type" text,
#                "Nullable" boolean) LANGUAGE SQL STABLE AS
#    $BODY$
#        SELECT a.attname,
#        format_type(t.oid, t.typtypmod),
#        a.attnotnull
#        FROM pg_attribute a, pg_type t
#        WHERE a.attrelid = rel
#        AND a.atttypid = t.oid
#        AND a.attnum >= 0
#        ORDER BY a.attnum;
#    $BODY$;
#
#    CREATE OR REPLACE FUNCTION show_indexesp(pattern text)
#    RETURNS TABLE("Table" regclass,
#                "Index" regclass,
#                "Columns" name[],
#                "Expr" text,
#                "Unique" boolean,
#                "Primary" boolean,
#                "Exclusion" boolean,
#                "Tablespace" name) LANGUAGE PLPGSQL STABLE AS
#    $BODY$
#    DECLARE
#        schema_name name = split_part(pattern, '.', 1);
#        table_name name = split_part(pattern, '.', 2);
#    BEGIN
#        IF schema_name = '' OR table_name = '' THEN
#        schema_name := current_schema();
#        table_name := pattern;
#        END IF;
#
#        RETURN QUERY
#        SELECT c.oid::regclass,
#        i.indexrelid::regclass,
#        array(SELECT "Column" FROM show_columns(i.indexrelid)),
#        pg_get_expr(i.indexprs, c.oid, true),
#        i.indisunique,
#        i.indisprimary,
#        i.indisexclusion,
#        (SELECT t.spcname FROM pg_class cc, pg_tablespace t WHERE cc.oid = i.indexrelid AND t.oid = cc.reltablespace)
#        FROM pg_class c, pg_index i
#        WHERE format('%I.%I', c.relnamespace::regnamespace::name, c.relname) LIKE format('%I.%s', schema_name, table_name)
#        AND c.oid = i.indrelid
#        ORDER BY c.oid, i.indexrelid;
#    END
#    $BODY$;

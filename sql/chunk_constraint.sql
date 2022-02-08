-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create constraint on newly created chunk based on hypertable constraint
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    constraint_type CHAR;
    check_sql TEXT;
    def TEXT;
    indx_tablespace NAME;
    tablespace_def TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
        check_sql = _timescaledb_internal.dimension_slice_get_constraint_sql(chunk_constraint_row.dimension_slice_id);
        IF check_sql IS NOT NULL THEN
            def := format('CHECK (%s)',  check_sql);
        ELSE
            def := NULL;
        END IF;
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN

        SELECT oid, contype INTO STRICT constraint_oid, constraint_type FROM pg_constraint
        WHERE conname=chunk_constraint_row.hypertable_constraint_name AND
              conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;

        IF constraint_type IN ('p','u') THEN
          -- since primary keys and unique constraints are backed by an index
          -- they might have an index tablespace assigned
          -- the tablspace is not part of the constraint definition so
          -- we have to append it explicitly to preserve it
          SELECT T.spcname INTO indx_tablespace
          FROM pg_constraint C, pg_class I, pg_tablespace T
          WHERE C.oid = constraint_oid AND C.contype IN ('p', 'u') AND I.oid = C.conindid AND I.reltablespace = T.oid;

          def := pg_get_constraintdef(constraint_oid);

          IF indx_tablespace IS NOT NULL THEN
            def := format('%s USING INDEX TABLESPACE %I', def, indx_tablespace);
          END IF;

        ELSIF constraint_type = 't' THEN
          -- constraint triggers are copied separately with normal triggers
          def := NULL;
        ELSE
          def := pg_get_constraintdef(constraint_oid);
        END IF;

    ELSE
        RAISE 'unknown constraint type';
    END IF;

    IF def IS NOT NULL THEN
        -- to allow for custom types with operators outside of pg_catalog
        -- we set search_path to @extschema@
        SET LOCAL search_path TO @extschema@;
        EXECUTE pg_catalog.format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
        );
    END IF;
END
$BODY$ SET search_path TO pg_catalog;

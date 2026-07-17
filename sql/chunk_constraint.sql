-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Create a non-dimensional constraint on a chunk by copying the
-- definition from the matching hypertable constraint.
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_constraint_add_table_constraint(
    chunk_id integer,
    constraint_name name,
    hypertable_constraint_name name
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    chunk_schema NAME;
    chunk_table NAME;
    constraint_oid OID;
    constraint_type CHAR;
    def TEXT;
    indx_tablespace NAME;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;
    -- schema_name and table_name are derived from the chunk relation
    SELECT n.nspname, c.relname INTO STRICT chunk_schema, chunk_table
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = chunk_row.relid;

    SELECT oid, contype INTO STRICT constraint_oid, constraint_type FROM pg_constraint
    WHERE conname = hypertable_constraint_name AND
          conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;

    IF constraint_type IN ('p','u') THEN
      -- primary keys and unique constraints are backed by an index which
      -- might have an index tablespace assigned. The tablespace is not
      -- part of the constraint definition so we have to append it
      -- explicitly to preserve it.
      SELECT T.spcname INTO indx_tablespace
      FROM pg_constraint C, pg_class I, pg_tablespace T
      WHERE C.oid = constraint_oid AND C.contype IN ('p', 'u') AND I.oid = C.conindid AND I.reltablespace = T.oid;

      def := pg_get_constraintdef(constraint_oid);

    ELSIF constraint_type = 't' THEN
      -- constraint triggers are copied separately with normal triggers
      def := NULL;
    ELSE
      def := pg_get_constraintdef(constraint_oid);
    END IF;

    IF def IS NOT NULL THEN
        -- to allow for custom types with operators outside of pg_catalog
        -- we set search_path to @extschema@
        SET LOCAL search_path TO @extschema@, pg_temp;
        EXECUTE pg_catalog.format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_schema, chunk_table, constraint_name, def
        );

        IF indx_tablespace IS NOT NULL THEN
            EXECUTE pg_catalog.format(
                $$ ALTER INDEX %I.%I SET TABLESPACE %I $$,
                chunk_schema, constraint_name, indx_tablespace
            );
        END IF;
    END IF;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Clone fk constraint from a hypertable to a compressed chunk
CREATE OR REPLACE FUNCTION _timescaledb_functions.constraint_clone(
    constraint_oid OID,
    target_oid REGCLASS
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    constraint_name NAME;
    def TEXT;
BEGIN
    def := pg_get_constraintdef(constraint_oid);
    SELECT conname INTO STRICT constraint_name FROM pg_constraint WHERE oid = constraint_oid;

    IF def IS NULL THEN
        RAISE 'constraint not found';
    END IF;

    -- to allow for custom types with operators outside of pg_catalog
    -- we set search_path to @extschema@
    SET LOCAL search_path TO @extschema@, pg_temp;
    EXECUTE pg_catalog.format($$ ALTER TABLE %s ADD CONSTRAINT %I %s $$, target_oid::pg_catalog.text, constraint_name, def);

END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create constraint on newly created chunk based on hypertable constraint
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint,
    using_index BOOLEAN
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
    chunk_constraint_index_name NAME;
    row_record RECORD;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
	    RAISE 'cannot create dimension constraint %', chunk_constraint_row;
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN

        SELECT oid, contype INTO STRICT constraint_oid, constraint_type FROM pg_constraint
        WHERE conname=chunk_constraint_row.hypertable_constraint_name AND
              conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;

        IF constraint_type IN ('p','u') THEN
          IF using_index THEN
            -- indexes created for constraints are named after the constraint
            -- so we can find the index name by looking up the constraint name.
            SELECT index_name INTO STRICT chunk_constraint_index_name FROM _timescaledb_catalog.chunk_index
            WHERE chunk_id = chunk_row.id AND hypertable_index_name = chunk_constraint_row.hypertable_constraint_name;

            IF chunk_constraint_index_name IS NULL THEN
              RAISE 'index not found for constraint %', chunk_constraint_row;
            END IF;

            CASE constraint_type
              WHEN 'p' THEN
                def := pg_catalog.format(
                    $$ PRIMARY KEY USING INDEX %I $$,
                    chunk_constraint_index_name
                );
              WHEN 'u' THEN
                def := pg_catalog.format(
                    $$ UNIQUE USING INDEX %I $$,
                    chunk_constraint_index_name
                );
            END CASE;
          ELSE
            -- since primary keys and unique constraints are backed by an index
            -- they might have an index tablespace assigned
            -- the tablspace is not part of the constraint definition so
            -- we have to append it explicitly to preserve it
            SELECT T.spcname INTO indx_tablespace
            FROM pg_constraint C, pg_class I, pg_tablespace T
            WHERE C.oid = constraint_oid AND C.contype IN ('p', 'u') AND I.oid = C.conindid AND I.reltablespace = T.oid;

            def := pg_get_constraintdef(constraint_oid);
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
        SET LOCAL search_path TO @extschema@, pg_temp;
        EXECUTE pg_catalog.format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
        );

        -- if constraint (primary or unique) needs a tablespace then add it
        -- via a separate ALTER INDEX SET TABLESPACE command. We cannot append it
        -- to the "def" string above since it leads to a SYNTAX error when
        -- "DEFERRABLE" or "INITIALLY DEFERRED" are used in the constraint
        IF indx_tablespace IS NOT NULL THEN
            EXECUTE pg_catalog.format(
                $$ ALTER INDEX %I.%I SET TABLESPACE %I $$,
                chunk_row.schema_name, chunk_constraint_row.constraint_name, indx_tablespace
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

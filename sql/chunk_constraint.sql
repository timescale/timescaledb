-- Creates a constraint on a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    def TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
        def := format('CHECK (%s)',  _timescaledb_internal.dimension_slice_get_constraint_sql(chunk_constraint_row.dimension_slice_id));
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN
        SELECT oid INTO STRICT constraint_oid FROM pg_constraint
        WHERE conname=chunk_constraint_row.hypertable_constraint_name AND
              conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;
        def := pg_get_constraintdef(constraint_oid);
    ELSE
        RAISE 'Unknown constraint type';
    END IF;

    sql_code := format(
        $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
        chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
    );
    EXECUTE sql_code;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_drop_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;

    sql_code := format(
        $$ ALTER TABLE %I.%I DROP CONSTRAINT %I $$,
        chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name
    );

    EXECUTE sql_code;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_constraint(
    chunk_id INTEGER,
    constraint_oid OID
)
    RETURNS OID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_constraint_row _timescaledb_catalog.chunk_constraint;
    chunk_row _timescaledb_catalog.chunk;
    constraint_row pg_constraint;
    hypertable_index_class_row pg_class;
    chunk_index_class_row pg_class;
    constraint_name TEXT;
    hypertable_constraint_name TEXT = NULL;
    chunk_constraint_oid OID;
BEGIN
    SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;
    hypertable_constraint_name := constraint_row.conname;
    constraint_name := format('%s_%s_%s', chunk_id,  nextval('_timescaledb_catalog.chunk_constraint_name'), hypertable_constraint_name);

    INSERT INTO _timescaledb_catalog.chunk_constraint (chunk_id, constraint_name, dimension_slice_id, hypertable_constraint_name)
    VALUES (chunk_id, constraint_name, NULL, hypertable_constraint_name) RETURNING * INTO STRICT chunk_constraint_row;

    --create actual constraint
    PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row);

    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk chunk WHERE chunk.id = chunk_id;

    SELECT oid INTO STRICT chunk_constraint_oid
    FROM pg_constraint con
    WHERE con.conrelid = format('%I.%I', chunk_row.schema_name, chunk_row.table_name)::regclass
    AND con.conname = constraint_name;

    RETURN chunk_constraint_oid;
END
$BODY$;

-- Drop a constraint on a chunk
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_constraint(
    chunk_id INTEGER,
    constraint_name NAME,
    alter_table BOOLEAN = true
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    chunk_constraint_row _timescaledb_catalog.chunk_constraint;
    constraint_row pg_constraint;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_id;

    DELETE FROM _timescaledb_catalog.chunk_constraint cc
    WHERE  cc.constraint_name = drop_chunk_constraint.constraint_name
    AND cc.chunk_id = drop_chunk_constraint.chunk_id
    RETURNING * INTO STRICT chunk_constraint_row;

    SELECT * INTO STRICT constraint_row
    FROM pg_constraint con
    WHERE con.conrelid = format('%I.%I', chunk_row.schema_name, chunk_row.table_name)::regclass
    AND con.conname = constraint_name;

    IF alter_table THEN
        EXECUTE format(
            $$  ALTER TABLE %I.%I DROP CONSTRAINT %I $$,
                chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name
        );
    END IF;

END
$BODY$;

-- Drops constraint on all chunks for a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_constraint(
    hypertable_id INTEGER,
    hypertable_constraint_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.drop_chunk_constraint(cc.chunk_id, cc.constraint_name)
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
    WHERE c.hypertable_id = drop_constraint.hypertable_id AND cc.hypertable_constraint_name = drop_constraint.hypertable_constraint_name;
END
$BODY$;

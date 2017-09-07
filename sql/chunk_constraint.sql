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
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_constraint_row _timescaledb_catalog.chunk_constraint;
    constraint_row pg_constraint;
    constraint_name TEXT;
    hypertable_constraint_name TEXT = NULL;
BEGIN
    SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;
    hypertable_constraint_name := constraint_row.conname;
    constraint_name := format('%s_%s_%s', chunk_id,  nextval('_timescaledb_catalog.chunk_constraint_name'), hypertable_constraint_name); 

    INSERT INTO _timescaledb_catalog.chunk_constraint (chunk_id, constraint_name, dimension_slice_id, hypertable_constraint_name)
    VALUES (chunk_id, constraint_name, NULL, hypertable_constraint_name) RETURNING * INTO STRICT chunk_constraint_row;

    PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row);
END
$BODY$;

-- Drop a constraint on a chunk
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_constraint(
    chunk_id INTEGER,
    constraint_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    chunk_constraint_row _timescaledb_catalog.chunk_constraint;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_id;

    DELETE FROM _timescaledb_catalog.chunk_constraint cc 
    WHERE  cc.constraint_name = drop_chunk_constraint.constraint_name 
    AND cc.chunk_id = drop_chunk_constraint.chunk_id
    RETURNING * INTO STRICT chunk_constraint_row;

    EXECUTE format(
        $$  ALTER TABLE %I.%I DROP CONSTRAINT %I $$,
            chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name
    );

END
$BODY$;

-- do I need to add a hypertable constraint to the chunks?;
CREATE OR REPLACE FUNCTION _timescaledb_internal.need_chunk_constraint(
    constraint_oid OID
)
    RETURNS BOOLEAN LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_row record;
BEGIN
    SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;

    IF constraint_row.contype IN ('c') THEN
        -- check and not null constraints handled by regular inheritance (from docs):
        --    All check constraints and not-null constraints on a parent table are automatically inherited by its children,
        --    unless explicitly specified otherwise with NO INHERIT clauses. Other types of constraints
        --    (unique, primary key, and foreign key constraints) are not inherited."

        IF constraint_row.connoinherit THEN
            RAISE 'NO INHERIT option not supported on hypertables: %', constraint_row.conname
            USING ERRCODE = 'IO101';
        END IF;

        RETURN FALSE;
    END IF;
    RETURN TRUE;
END
$BODY$;

-- Creates a constraint on all chunks for a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.add_constraint(
    hypertable_id INTEGER,
    constraint_oid OID
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_row pg_constraint;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    IF _timescaledb_internal.need_chunk_constraint(constraint_oid) THEN
        SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;

        --check the validity of an index if a constraint uses an index
        --note: foreign-key constraints are excluded because they point to indexes on the foreign table /not/ the hypertable
        IF constraint_row.conindid <> 0 AND constraint_row.contype != 'f' THEN
            SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = hypertable_id;
            PERFORM _timescaledb_internal.check_index(constraint_row.conindid, hypertable_row);
        END IF;

        PERFORM _timescaledb_internal.create_chunk_constraint(c.id, constraint_oid)
        FROM _timescaledb_catalog.chunk c
        WHERE c.hypertable_id = add_constraint.hypertable_id;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.add_constraint_by_name(
    hypertable_id INTEGER,
    constraint_name name
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_oid OID;
BEGIN
    SELECT oid INTO STRICT constraint_oid FROM pg_constraint WHERE conname = constraint_name 
    AND conrelid = _timescaledb_internal.main_table_from_hypertable(hypertable_id);

    PERFORM _timescaledb_internal.add_constraint(hypertable_id, constraint_oid);
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


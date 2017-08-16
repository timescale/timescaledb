-- Creates a constraint on a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_constraint(
    chunk_id INTEGER,
    constraint_oid OID
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
    constraint_row pg_constraint;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk WHERE id = chunk_id;

    INSERT INTO _timescaledb_catalog.chunk_constraint (hypertable_constraint_name, chunk_id, constraint_name)
    SELECT constraint_row.conname, chunk_row.id, format('%s_%s_%s', chunk_id,  nextval('_timescaledb_catalog.chunk_constraint_name'), constraint_row.conname);
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
BEGIN
    DELETE FROM _timescaledb_catalog.chunk_constraint c
    WHERE c.hypertable_constraint_name = drop_chunk_constraint.constraint_name 
    AND c.chunk_id = drop_chunk_constraint.chunk_id;
END
$BODY$;

-- do I need to add a hypertable constraint to the chunks?;
CREATE OR REPLACE FUNCTION _timescaledb_internal.need_chunk_constraint(
    hypertable_id INTEGER,
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
    IF _timescaledb_internal.need_chunk_constraint(hypertable_id, constraint_oid) THEN
        SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;

        --check the validity of an index if a constrain uses an index
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
    constraint_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.drop_chunk_constraint(c.id, constraint_name)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = drop_constraint.hypertable_id;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk_constraint()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    constraint_oid OID;
    ds_row _timescaledb_catalog.dimension_slice;
    sql_code TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = NEW.chunk_id;
        IF NEW.dimension_slice_id IS NOT NULL THEN
            SELECT * INTO STRICT ds_row FROM _timescaledb_catalog.dimension_slice ds WHERE ds.id = NEW.dimension_slice_id;
            EXECUTE format(
                $$
                    ALTER TABLE %1$I.%2$I
                    ADD CONSTRAINT %3$s CHECK(%4$s)
                $$,
                chunk_row.schema_name, chunk_row.table_name,
                NEW.constraint_name,
                _timescaledb_internal.dimension_slice_get_constraint_sql(ds_row.id)
            );
        ELSIF NEW.hypertable_constraint_name IS NOT NULL THEN
            SELECT con.oid INTO STRICT constraint_oid 
            FROM _timescaledb_catalog.chunk c
            INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = c.hypertable_id)
            INNER JOIN pg_constraint con ON (con.conrelid = format('%I.%I',h.schema_name, h.table_name)::regclass 
                AND con.conname = NEW.hypertable_constraint_name)
            WHERE c.id = NEW.chunk_id;

            sql_code := format($$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_row.schema_name, chunk_row.table_name, NEW.constraint_name, pg_get_constraintdef(constraint_oid)
            );
            EXECUTE sql_code;
        ELSE
            RAISE 'Unknown constraint type';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        SELECT * INTO chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = OLD.chunk_id;
        IF FOUND THEN
            EXECUTE format($$  ALTER TABLE %I.%I DROP CONSTRAINT %I $$,
                chunk_row.schema_name, chunk_row.table_name, OLD.constraint_name
            );
        END IF;
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;


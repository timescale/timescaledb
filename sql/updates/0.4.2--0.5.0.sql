DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_is_change_owner(pg_ddl_command);
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_change_owner_to(pg_ddl_command);

DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_add_constraints(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_alter_table() CASCADE;

CREATE INDEX ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id);

ALTER TABLE IF EXISTS _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_pkey,
ADD COLUMN constraint_name NAME;

UPDATE _timescaledb_catalog.chunk_constraint cc
SET constraint_name =
  (SELECT con.conname FROM
   _timescaledb_catalog.chunk c
   INNER JOIN _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
   INNER JOIN _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
   INNER JOIN pg_constraint con ON (con.contype = 'c' AND con.conrelid = format('%I.%I',c.schema_name, c.table_name)::regclass)
   INNER JOIN pg_attribute att ON (att.attrelid = format('%I.%I',c.schema_name, c.table_name)::regclass AND att.attname = d.column_name)
   WHERE c.id = cc.chunk_id
   AND con.conname = format('constraint_%s', dimension_slice_id)
   AND array_length(con.conkey, 1) = 1 AND con.conkey = ARRAY[att.attnum]
   );

ALTER TABLE IF EXISTS _timescaledb_catalog.chunk_constraint
ALTER COLUMN constraint_name SET NOT NULL,
ALTER COLUMN dimension_slice_id DROP NOT NULL;

ALTER TABLE IF EXISTS _timescaledb_catalog.chunk_constraint
ADD COLUMN hypertable_constraint_name NAME NULL,
ADD CONSTRAINT chunk_constraint_chunk_id_constraint_name_key UNIQUE (chunk_id, constraint_name);

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

DROP FUNCTION IF EXISTS _timescaledb_internal.rename_hypertable(name, name, text, text);
DROP FUNCTION IF EXISTS create_hypertable(REGCLASS, NAME, NAME,INTEGER,NAME,NAME,BIGINT,BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS hypertable_relation_size(regclass);
DROP FUNCTION IF EXISTS chunk_relation_size(regclass);
DROP FUNCTION IF EXISTS indexes_relation_size(regclass);

---- Post script
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create_after_lock(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range_closed(BIGINT, SMALLINT, BIGINT, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range(INTEGER, BIGINT, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_calculate_new_ranges(INTEGER, BIGINT, INTEGER[], BIGINT[], BOOLEAN, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_id_get_by_dimensions(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get_dimensions_constraint_sql(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get_dimension_constraint_sql(INTEGER, BIGINT) CASCADE;

--Makes sure the index is valid for a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.check_index(index_oid REGCLASS, hypertable_row  _timescaledb_catalog.hypertable)
RETURNS VOID LANGUAGE plpgsql STABLE AS
$BODY$
DECLARE
    index_row       RECORD;
    missing_column  TEXT;
BEGIN
    SELECT * INTO STRICT index_row FROM pg_index WHERE indexrelid = index_oid;
    IF index_row.indisunique OR index_row.indisexclusion THEN
        -- unique/exclusion index must contain time and all partition dimension columns.

        -- get any partitioning columns that are not included in the index.
        SELECT d.column_name INTO missing_column
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = hypertable_row.id AND
              d.column_name NOT IN (
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = index_row.indrelid AND
                attnum = ANY(index_row.indkey)
            );

        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'Cannot create a unique index without the column: % (used in partitioning)', missing_column
            USING ERRCODE = 'TS103';
        END IF;
    END IF;
END
$BODY$;

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
            USING ERRCODE = 'TS101';
        END IF;

        RETURN FALSE;
    END IF;
    RETURN TRUE;
END
$BODY$;

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


SELECT _timescaledb_internal.add_constraint(h.id, c.oid)
FROM _timescaledb_catalog.hypertable h
INNER JOIN pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass);

DELETE FROM _timescaledb_catalog.hypertable_index hi
WHERE EXISTS (
 SELECT 1 FROM pg_constraint WHERE conindid = format('%I.%I', hi.main_schema_name, hi.main_index_name)::regclass
);

ALTER TABLE IF EXISTS _timescaledb_catalog.chunk
DROP CONSTRAINT chunk_hypertable_id_fkey,
ADD CONSTRAINT chunk_hypertable_id_fkey
  FOREIGN KEY (hypertable_id)
  REFERENCES _timescaledb_catalog.hypertable(id);

ALTER TABLE IF EXISTS  _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_chunk_id_fkey,
ADD CONSTRAINT chunk_constraint_chunk_id_fkey
  FOREIGN KEY (chunk_id)
  REFERENCES _timescaledb_catalog.chunk(id);

ALTER TABLE IF EXISTS  _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey,
ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
  FOREIGN KEY (dimension_slice_id)
  REFERENCES _timescaledb_catalog.dimension_slice(id);


DROP EVENT TRIGGER IF EXISTS ddl_check_drop_command;

DROP TRIGGER IF EXISTS trigger_main_on_change_chunk ON _timescaledb_catalog.chunk;

DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create_table(int);
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_drop_table();
DROP FUNCTION IF EXISTS _timescaledb_internal.on_change_chunk();
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_hypertable(name, name);

DROP EVENT TRIGGER IF EXISTS ddl_create_trigger;
DROP EVENT TRIGGER IF EXISTS ddl_drop_trigger;

DROP FUNCTION IF EXISTS _timescaledb_internal.add_trigger(int, oid);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_trigger(int, name, text);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_trigger_on_all_chunks(int, name, text);
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_create_trigger();
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_process_drop_trigger();
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunk_trigger(int, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_trigger_on_all_chunks(INTEGER, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.get_general_trigger_definition(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.get_trigger_definition_for_table(INTEGER, text);
DROP FUNCTION IF EXISTS _timescaledb_internal.need_chunk_trigger(int, oid);

-- Adding this in the update script because aggregates.sql is not rerun in case of an update
CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'ts_hist_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'ts_hist_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'ts_hist_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'ts_hist_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTEGER[]
AS '@MODULE_PATHNAME@', 'ts_hist_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- This aggregate partitions the dataset into a specified number of buckets (nbuckets) ranging
-- from the inputted min to max values.
CREATE AGGREGATE histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER) (
    SFUNC = _timescaledb_internal.hist_sfunc,
    STYPE = INTERNAL,
    COMBINEFUNC = _timescaledb_internal.hist_combinefunc,
    SERIALFUNC = _timescaledb_internal.hist_serializefunc,
    DESERIALFUNC = _timescaledb_internal.hist_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_internal.hist_finalfunc,
    FINALFUNC_EXTRA
);

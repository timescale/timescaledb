CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_get_time(
    hypertable_id INT
)
    RETURNS _timescaledb_catalog.dimension LANGUAGE SQL STABLE AS
$BODY$
    SELECT *
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = dimension_get_time.hypertable_id AND
          d.interval_length IS NOT NULL
$BODY$;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table. Any of the first three arguments
-- can be NULL meaning "all values".
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_impl(
    older_than_time  BIGINT,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    truncate_before  BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    cascade_mod TEXT = '';
    exist_count INT = 0;
BEGIN
    IF older_than_time IS NULL AND table_name IS NULL AND schema_name IS NULL THEN
        RAISE 'cannot have all 3 arguments to drop_chunks_older_than be NULL';
    END IF;

    IF cascade THEN
        cascade_mod = 'CASCADE';
    END IF;

    IF table_name IS NOT NULL THEN
        SELECT COUNT(*)
        FROM _timescaledb_catalog.hypertable h
        WHERE (drop_chunks_impl.schema_name IS NULL OR h.schema_name = drop_chunks_impl.schema_name)
        AND drop_chunks_impl.table_name = h.table_name
        INTO STRICT exist_count;

        IF exist_count = 0 THEN
            RAISE 'hypertable "%" does not exist', drop_chunks_impl.table_name
            USING ERRCODE = 'IO001';
        END IF;
    END IF;

    FOR chunk_row IN SELECT *
        FROM _timescaledb_catalog.chunk c
        INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = c.hypertable_id)
        INNER JOIN _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
        INNER JOIN _timescaledb_catalog.dimension_slice ds
            ON (ds.dimension_id = time_dimension.id)
        INNER JOIN _timescaledb_catalog.chunk_constraint cc
            ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
        WHERE (older_than_time IS NULL OR ds.range_end <= older_than_time)
        AND (drop_chunks_impl.schema_name IS NULL OR h.schema_name = drop_chunks_impl.schema_name)
        AND (drop_chunks_impl.table_name IS NULL OR h.table_name = drop_chunks_impl.table_name)
    LOOP
        IF truncate_before THEN
            EXECUTE format(
                $$
                TRUNCATE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
            );
        END IF;
        EXECUTE format(
                $$
                DROP TABLE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
        );
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_type_check(
    given_type REGTYPE,
    table_name  NAME,
    schema_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    actual_type regtype;
BEGIN
    BEGIN
        WITH hypertable_ids AS (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE (drop_chunks_type_check.schema_name IS NULL OR h.schema_name = drop_chunks_type_check.schema_name) AND
            (drop_chunks_type_check.table_name IS NULL OR h.table_name = drop_chunks_type_check.table_name)
        )
        SELECT DISTINCT time_dim.column_type INTO STRICT actual_type
        FROM hypertable_ids INNER JOIN LATERAL _timescaledb_internal.dimension_get_time(hypertable_ids.id) time_dim ON (true);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'no hypertables found';
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'cannot use drop_chunks on multiple tables with different time types';
    END;

    IF given_type IN ('int'::regtype, 'smallint'::regtype, 'bigint'::regtype ) THEN
        IF actual_type IN ('int'::regtype, 'smallint'::regtype, 'bigint'::regtype ) THEN
            RETURN;
        END IF;
    END IF;
    IF actual_type != given_type THEN
        RAISE EXCEPTION 'cannot call drop_chunks with a % on hypertables with a time type of: %', given_type, actual_type;
    END IF;
END
$BODY$;

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'chunk_index_replace' LANGUAGE C VOLATILE STRICT;

-- select_tablespace() is used to assign a tablespace to a chunk.  A
-- tablespace is selected from a set of tablespaces associated with
-- the chunk's hypertable, if any.
CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    hypertable_id INTEGER,
    chunk_dimension_slice_ids      INTEGER[]
)
    RETURNS NAME LANGUAGE PLPGSQL VOLATILE AS
$BODY$
<<main_block>>
DECLARE
    chunk_slice_id INTEGER;
    chunk_slice_index INTEGER;
    dimension_id INTEGER;
    dimension_slice_id INTEGER;
    tablespaces NAME[] = ARRAY(
        SELECT t.tablespace_name FROM _timescaledb_catalog.tablespace t
        WHERE (t.hypertable_id = select_tablespace.hypertable_id)
        ORDER BY id DESC
    );
    all_dimension_slice_ids_for_dimension INT[];
    partitioning_func TEXT;
BEGIN

    IF array_length(tablespaces, 1) = 0 THEN
       RETURN NULL;
    END IF;

    -- Try to pick first closed dimension, otherwise first open.
    -- The partition_func variable will be valid or NULL depending
    -- on the type of dimension found. This can be used to pick
    -- different tablespace assignment strategies depending
    -- on type of dimension.
    SELECT d.id, d.partitioning_func FROM _timescaledb_catalog.dimension d
    WHERE (d.hypertable_id = select_tablespace.hypertable_id)
    ORDER BY partitioning_func NULLS LAST, id DESC
    LIMIT 1
    INTO dimension_id, partitioning_func;

    -- Find all dimension slices for the chosen dimension
    all_dimension_slice_ids_for_dimension := ARRAY(
        SELECT s.id FROM _timescaledb_catalog.dimension_slice s
        WHERE (s.dimension_id = main_block.dimension_id)
        ORDER BY s.id
    );

    -- Find the array index of the chunk's dimension slice
    -- Here chunk_dimension_slice_ids has one slice per dimension of the hypercube
    -- while all_dimension_slice_ids_for_dimension has all the slice for a single dimension.
    SELECT i
    FROM generate_subscripts(all_dimension_slice_ids_for_dimension, 1) AS i
    WHERE all_dimension_slice_ids_for_dimension[i] = ANY(chunk_dimension_slice_ids)
    INTO STRICT chunk_slice_index;

    -- Use the chunk's dimension slice index to pick a tablespace in
    -- the tablespaces array
    RETURN tablespaces[chunk_slice_index % array_length(tablespaces, 1) + 1];
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    hypertable_id INTEGER,
    chunk_id      INTEGER
)
    RETURNS NAME LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_dimension_slice_ids INTEGER[];
BEGIN
    -- Find the chunk's dimension slice ids
    chunk_dimension_slice_ids = ARRAY (
        SELECT s.id FROM _timescaledb_catalog.dimension_slice s
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = s.id)
        INNER JOIN _timescaledb_catalog.chunk c ON (cc.chunk_id = c.id)
        WHERE (c.id = select_tablespace.chunk_id)
    );

    RETURN _timescaledb_internal.select_tablespace(hypertable_id, chunk_dimension_slice_ids);
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    chunk_id INTEGER
)
    RETURNS NAME LANGUAGE SQL AS
$BODY$
    SELECT _timescaledb_internal.select_tablespace(
        (SELECT hypertable_id FROM _timescaledb_catalog.chunk WHERE id = chunk_id),
        chunk_id);
$BODY$;

CREATE FUNCTION _timescaledb_internal.relation_size(relation REGCLASS)
RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT)
AS '@MODULE_PATHNAME@', 'ts_relation_size' LANGUAGE C VOLATILE;

DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP INDEX IF EXISTS _timescaledb_catalog.chunk_constraint_chunk_id_dimension_slice_id_idx;
CREATE INDEX chunk_constraint_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (dimension_slice_id);

-- Report the compressed chunks that have a wrong collation. See https://github.com/timescale/timescaledb/pull/4236
DO $$
DECLARE
    _hypertable regclass;
    _column_name text;
    _chunks regclass[];
BEGIN
    FOR _hypertable,
    _column_name,
    _chunks IN
-- We materialize this CTE so that the filter on dropped chunks works
-- first, and we don't try to look up regclass for dropped chunks.
WITH chunk AS MATERIALIZED (
        SELECT
            format('%I.%I', compressed_chunk.schema_name, compressed_chunk.table_name) compressed_chunk,
            format('%I.%I', normal_chunk.schema_name, normal_chunk.table_name) normal_chunk,
            normal_chunk.hypertable_id hypertable_id
        FROM
            _timescaledb_catalog.chunk normal_chunk,
            _timescaledb_catalog.chunk compressed_chunk
        WHERE
            normal_chunk.compressed_chunk_id = compressed_chunk.id
            AND NOT normal_chunk.dropped
),
col AS (
    SELECT
        hypertable_id,
        normal_chunk,
        normal_column.attname column_name
    FROM
        chunk,
        pg_attribute normal_column,
        pg_attribute compressed_column
    WHERE
        normal_column.attrelid = normal_chunk::regclass
        AND compressed_column.attrelid = compressed_chunk::regclass
        AND normal_column.attname = compressed_column.attname
        AND compressed_column.atttypid != '_timescaledb_internal.compressed_data'::regtype
        AND normal_column.attcollation != compressed_column.attcollation
),
report_rows AS (
    SELECT
        format('%I.%I', schema_name, table_name)::regclass hypertable,
        normal_chunk::regclass chunk,
        column_name
    FROM
        col,
        _timescaledb_catalog.hypertable
    WHERE
        hypertable.id = hypertable_id
)
SELECT
    hypertable,
    column_name,
    array_agg(chunk) chunks
FROM
    report_rows
GROUP BY
    hypertable,
    column_name LOOP
        RAISE warning 'some compressed chunks for hypertable "%" use a wrong collation for the column "%"', _hypertable, _column_name
            USING detail = 'This may lead to wrong order of results if you are using an index on this column of the compessed chunk.',
            hint = format('If you experience this problem, disable compression on the table and enable it again. This will require decompressing and compressing all chunks of the table. The affected chunks are "%s".', _chunks);
END LOOP;
END
$$;

-- Get rid of chunk_id from materialization hypertables
DROP FUNCTION IF EXISTS timescaledb_experimental.refresh_continuous_aggregate(REGCLASS, REGCLASS);

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

ALTER TABLE _timescaledb_catalog.continuous_agg
  ADD COLUMN finalized BOOL;

UPDATE _timescaledb_catalog.continuous_agg SET finalized = FALSE;

ALTER TABLE _timescaledb_catalog.continuous_agg
  ALTER COLUMN finalized SET NOT NULL,
  ALTER COLUMN finalized SET DEFAULT TRUE;

DROP PROCEDURE IF EXISTS timescaledb_experimental.move_chunk(REGCLASS, NAME, NAME);
DROP PROCEDURE IF EXISTS timescaledb_experimental.copy_chunk(REGCLASS, NAME, NAME);

CREATE OR REPLACE FUNCTION timescaledb_experimental.subscription_exec(
    subscription_command TEXT
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_subscription_exec' LANGUAGE C VOLATILE;

-- Recreate chunk_copy_operation table with newly added `compress_chunk_name` column
--

CREATE TABLE _timescaledb_catalog._tmp_chunk_copy_operation (
  operation_id name NOT NULL,
  backend_pid integer NOT NULL,
  completed_stage name NOT NULL,
  time_start timestamptz NOT NULL DEFAULT NOW(),
  chunk_id integer NOT NULL,
  compress_chunk_name name NOT NULL, -- new column
  source_node_name name NOT NULL,
  dest_node_name name NOT NULL,
  delete_on_source_node bool NOT NULL
);

INSERT INTO _timescaledb_catalog._tmp_chunk_copy_operation
    SELECT
        operation_id,
        backend_pid,
        completed_stage,
        time_start,
        chunk_id,
        '', -- compress_chunk_name
        source_node_name,
        dest_node_name,
        delete_on_source_node
    FROM
        _timescaledb_catalog.chunk_copy_operation
    ORDER BY
        operation_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.chunk_copy_operation;

DROP TABLE _timescaledb_catalog.chunk_copy_operation;

-- Create a new table to void doing rename operation on the tmp table
--
CREATE TABLE _timescaledb_catalog.chunk_copy_operation (
  operation_id name NOT NULL,
  backend_pid integer NOT NULL,
  completed_stage name NOT NULL,
  time_start timestamptz NOT NULL DEFAULT NOW(),
  chunk_id integer NOT NULL,
  compress_chunk_name name NOT NULL,
  source_node_name name NOT NULL,
  dest_node_name name NOT NULL,
  delete_on_source_node bool NOT NULL
);

INSERT INTO _timescaledb_catalog.chunk_copy_operation
    SELECT
        operation_id,
        backend_pid,
        completed_stage,
        time_start,
        chunk_id,
        compress_chunk_name,
        source_node_name,
        dest_node_name,
        delete_on_source_node
    FROM
        _timescaledb_catalog._tmp_chunk_copy_operation
    ORDER BY
        operation_id;

DROP TABLE _timescaledb_catalog._tmp_chunk_copy_operation;

ALTER TABLE _timescaledb_catalog.chunk_copy_operation
    ADD CONSTRAINT chunk_copy_operation_pkey PRIMARY KEY (operation_id),
    ADD CONSTRAINT chunk_copy_operation_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;

GRANT SELECT ON TABLE _timescaledb_catalog.chunk_copy_operation TO PUBLIC;

ANALYZE _timescaledb_catalog.chunk_copy_operation;

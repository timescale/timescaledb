DROP FUNCTION IF EXISTS drop_chunks(INTEGER, NAME, NAME, BOOLEAN);

DROP FUNCTION _timescaledb_internal.create_chunk_constraint(integer,oid);
DROP FUNCTION _timescaledb_internal.add_constraint(integer,oid);
DROP FUNCTION _timescaledb_internal.add_constraint_by_name(integer,name);
DROP FUNCTION _timescaledb_internal.need_chunk_constraint(oid);

INSERT INTO _timescaledb_catalog.chunk_index (chunk_id, index_name, hypertable_id, hypertable_index_name)
SELECT chunk_con.chunk_id, pg_chunk_index_class.relname, chunk.hypertable_id, pg_hypertable_index_class.relname
FROM _timescaledb_catalog.chunk_constraint chunk_con
INNER JOIN _timescaledb_catalog.chunk chunk ON (chunk_con.chunk_id = chunk.id)
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
INNER JOIN pg_constraint pg_chunk_con ON (
        pg_chunk_con.conrelid = format('%I.%I', chunk.schema_name, chunk.table_name)::regclass
        AND pg_chunk_con.conname = chunk_con.constraint_name
)
INNER JOIN pg_class pg_chunk_index_class ON (
    pg_chunk_con.conindid = pg_chunk_index_class.oid
)
INNER JOIN pg_constraint pg_hypertable_con ON (
        pg_hypertable_con.conrelid = format('%I.%I', hypertable.schema_name, hypertable.table_name)::regclass
        AND pg_hypertable_con.conname = chunk_con.hypertable_constraint_name
)
INNER JOIN pg_class pg_hypertable_index_class ON (
    pg_hypertable_con.conindid = pg_hypertable_index_class.oid
);

-- Upgrade support for setting partitioning function
DROP FUNCTION create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean);
DROP FUNCTION add_dimension(regclass,name,integer,bigint);
DROP FUNCTION _timescaledb_internal.create_hypertable_row(regclass,name,name,name,name,integer,name,name,bigint,name);
DROP FUNCTION _timescaledb_internal.add_dimension(regclass,_timescaledb_catalog.hypertable,name,integer,bigint,boolean);

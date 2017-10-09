-- Indexing updates

-- Convert old chunk_index table data to new format
INSERT INTO _timescaledb_catalog.chunk_index
SELECT ch.id, ci.index_name, h.id, ci.main_index_name
FROM _timescaledb_catalog.chunk_index_old ci,
     _timescaledb_catalog.hypertable h,
     _timescaledb_catalog.chunk ch,
     pg_index i,
     pg_class c
WHERE ci.schema_name = ch.schema_name
AND   ci.table_name = ch.table_name
AND   i.indexrelid = format('%I.%I', ci.main_schema_name, ci.main_index_name)::REGCLASS
AND   i.indrelid = c.oid
AND   ci.main_schema_name = h.schema_name
AND   c.relname = h.table_name;

ALTER EXTENSION timescaledb
DROP TABLE _timescaledb_catalog.chunk_index_old;
ALTER EXTENSION timescaledb
DROP TABLE _timescaledb_catalog.hypertable_index;
ALTER EXTENSION timescaledb
DROP SEQUENCE _timescaledb_catalog.chunk_index_old_id_seq;

DROP TABLE _timescaledb_catalog.chunk_index_old;
DROP TABLE _timescaledb_catalog.hypertable_index;
-- No need to drop _timescaledb_catalog.chunk_index_old_id_seq,
-- removed with table.

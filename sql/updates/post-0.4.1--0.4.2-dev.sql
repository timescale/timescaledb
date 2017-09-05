CREATE TRIGGER trigger_main_on_change_chunk_constraint
AFTER UPDATE OR DELETE OR INSERT ON _timescaledb_catalog.chunk_constraint
FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_chunk_constraint();

SELECT _timescaledb_internal.add_constraint(h.id, c.oid)
FROM _timescaledb_catalog.hypertable h
INNER JOIN pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass);

DELETE FROM _timescaledb_catalog.hypertable_index hi
WHERE EXISTS (
 SELECT 1 FROM pg_constraint WHERE conindid = format('%I.%I', hi.main_schema_name, hi.main_index_name)::regclass
);


-- Add constraint name to chunk_constraint table
ALTER TABLE _timescaledb_catalog.chunk_constraint ADD COLUMN constraint_name NAME;
-- Fill in the constraint names from pg_constraint
UPDATE _timescaledb_catalog.chunk_constraint cc SET constraint_name =
(SELECT conname FROM pg_constraint co,
 pg_class cl,
 pg_namespace ns,
 pg_attribute att,
 _timescaledb_catalog.chunk ch,
 _timescaledb_catalog.dimension_slice ds,
 _timescaledb_catalog.dimension d
 WHERE cl.oid = co.conrelid
 AND cl.relnamespace = ns.oid
 AND ch.table_name = cl.relname
 AND ns.nspname = ch.schema_name
 AND cc.dimension_slice_id = ds.id
 AND ds.dimension_id = d.id
 AND att.attrelid = cl.oid
 AND att.attnum IN (SELECT unnest(conkey) FROM pg_constraint con WHERE con.oid = co.oid)
 AND att.attname = d.column_name
 AND cc.chunk_id = ch.id);
-- Add NOT NULL constraint on constraint_name column
ALTER TABLE _timescaledb_catalog.chunk_constraint ALTER COLUMN constraint_name SET NOT NULL;

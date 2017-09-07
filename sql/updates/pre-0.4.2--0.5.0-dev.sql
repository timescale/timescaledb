DROP FUNCTION _timescaledb_internal.ddl_is_change_owner(pg_ddl_command);
DROP FUNCTION _timescaledb_internal.ddl_change_owner_to(pg_ddl_command);

DROP FUNCTION _timescaledb_internal.chunk_add_constraints(integer);
DROP FUNCTION _timescaledb_internal.ddl_process_alter_table() CASCADE;

CREATE INDEX ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id);

ALTER TABLE _timescaledb_catalog.chunk_constraint
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

ALTER TABLE _timescaledb_catalog.chunk_constraint 
ALTER COLUMN constraint_name SET NOT NULL,
ALTER COLUMN dimension_slice_id DROP NOT NULL;

ALTER TABLE _timescaledb_catalog.chunk_constraint
ADD COLUMN hypertable_constraint_name NAME NULL,
ADD CONSTRAINT chunk_constraint_chunk_id_constraint_name_key UNIQUE (chunk_id, constraint_name);

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

DROP FUNCTION _timescaledb_internal.rename_hypertable(name, name, text, text);

DROP FUNCTION _timescaledb_internal.ddl_is_change_owner(pg_ddl_command);
DROP FUNCTION _timescaledb_internal.ddl_change_owner_to(pg_ddl_command);

DROP FUNCTION _timescaledb_internal.chunk_add_constraints(integer);
DROP FUNCTION _timescaledb_internal.ddl_process_alter_table() CASCADE;

CREATE INDEX ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id) WHERE dimension_slice_id IS NOT NULL;

ALTER TABLE _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_pkey,
ADD COLUMN constraint_name NAME;

UPDATE _timescaledb_catalog.chunk_constraint
SET constraint_name = format('constraint_%s', dimension_slice_id);

ALTER TABLE _timescaledb_catalog.chunk_constraint 
ALTER COLUMN constraint_name SET NOT NULL,
ALTER COLUMN dimension_slice_id DROP NOT NULL;

ALTER TABLE _timescaledb_catalog.chunk_constraint
ADD COLUMN hypertable_constraint_name NAME NULL,
ADD CONSTRAINT chunk_constraint_pkey PRIMARY KEY (chunk_id, constraint_name);

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

CREATE SCHEMA timescaledb_experimental;
GRANT USAGE ON SCHEMA timescaledb_experimental TO PUBLIC;
DROP FUNCTION IF EXISTS _timescaledb_internal.block_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.allow_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.refresh_continuous_aggregate;
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_catalog.chunk_copy_operation (
  operation_id name PRIMARY KEY, -- the publisher/subscriber identifier used
  backend_pid integer NOT NULL, -- the pid of the backend running this activity
  completed_stage name NOT NULL, -- the completed stage/step
  time_start timestamptz NOT NULL DEFAULT NOW(), -- start time of the activity
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  source_node_name name NOT NULL,
  dest_node_name name NOT NULL,
  delete_on_source_node bool NOT NULL -- is a move or copy activity
);

GRANT SELECT ON _timescaledb_catalog.chunk_copy_operation_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk_copy_operation TO PUBLIC;

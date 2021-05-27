CREATE SCHEMA IF NOT EXISTS timescaledb_experimental;
GRANT USAGE ON SCHEMA timescaledb_experimental TO PUBLIC;
DROP FUNCTION IF EXISTS _timescaledb_internal.block_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.allow_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk;

-- Use the experimental schema for ths new procedure
CREATE OR REPLACE PROCEDURE timescaledb_experimental.move_chunk(
    chunk REGCLASS,
    source_node Name = NULL,
    destination_node Name = NULL,
    verbose BOOLEAN=FALSE)
AS '@MODULE_PATHNAME@', 'ts_move_chunk_proc' LANGUAGE C;

CREATE SEQUENCE IF NOT EXISTS _timescaledb_catalog.chunk_copy_activity_id_seq MINVALUE 1;

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_copy_activity (
  id integer PRIMARY KEY DEFAULT nextval('_timescaledb_catalog.chunk_copy_activity_id_seq'),
  operation_id name NOT NULL UNIQUE, -- the publisher/subscriber identifier used
  backend_pid integer NOT NULL, -- the pid of the backend running this activity
  completed_stage text NOT NULL, -- the completed stage/step
  time_start timestamptz NOT NULL DEFAULT NOW(), -- start time of the activity
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  source_node_name name NOT NULL,
  dest_node_name name NOT NULL,
  delete_on_source_node bool NOT NULL -- is a move or copy activity
);

ALTER SEQUENCE _timescaledb_catalog.chunk_copy_activity_id_seq OWNED BY _timescaledb_catalog.chunk_copy_activity.id;

GRANT SELECT ON _timescaledb_catalog.chunk_copy_activity_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk_copy_activity TO PUBLIC;

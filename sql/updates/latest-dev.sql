
--
-- Rebuild the catalog table `_timescaledb_catalog.chunk` to drop column `dropped`
--

CREATE TABLE _timescaledb_internal.tmp_chunk AS SELECT * from _timescaledb_catalog.chunk WHERE NOT dropped;
CREATE TABLE _timescaledb_internal.tmp_chunk_seq_value AS SELECT last_value, is_called FROM _timescaledb_catalog.chunk_id_seq;

--drop foreign keys on chunk table
ALTER TABLE _timescaledb_catalog.chunk_constraint DROP CONSTRAINT chunk_constraint_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_column_stats DROP CONSTRAINT chunk_column_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_information.chunk_columnstore_settings;
DROP VIEW IF EXISTS timescaledb_information.chunk_compression_settings;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_id_seq;

DROP TABLE _timescaledb_catalog.chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

-- now create table without self referential foreign key
CREATE TABLE _timescaledb_catalog.chunk (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL,
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer ,
  status integer NOT NULL DEFAULT 0,
  osm_chunk boolean NOT NULL DEFAULT FALSE,
  creation_time timestamptz NOT NULL,
  -- table constraints
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_schema_name_table_name_key UNIQUE (schema_name, table_name)
);

INSERT INTO _timescaledb_catalog.chunk( id, hypertable_id, schema_name, table_name, compressed_chunk_id, status, osm_chunk, creation_time)
SELECT id, hypertable_id, schema_name, table_name, compressed_chunk_id, status, osm_chunk, creation_time
FROM _timescaledb_internal.tmp_chunk;

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);
CREATE INDEX chunk_hypertable_id_creation_time_idx ON _timescaledb_catalog.chunk(hypertable_id, creation_time);

ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;
SELECT setval('_timescaledb_catalog.chunk_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_chunk_seq_value;

-- add self referential foreign key
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY ( compressed_chunk_id ) REFERENCES _timescaledb_catalog.chunk( id );

--add foreign key constraint
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.chunk_constraint ADD CONSTRAINT chunk_constraint_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_catalog.chunk_column_stats ADD CONSTRAINT chunk_column_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id);
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal.tmp_chunk;
DROP TABLE _timescaledb_internal.tmp_chunk_seq_value;

GRANT SELECT ON _timescaledb_catalog.chunk_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk TO PUBLIC;
-- end rebuild _timescaledb_catalog.chunk table --

-- drop the catalog tables for continuous aggregate migration plans

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq;
DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step;
DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan;

--
-- Add this index to speed up queries for recent job history
-- This statement is idempotent to allow the index to have been precreated.
--
CREATE INDEX IF NOT EXISTS bgw_job_stat_history_execution_start_idx
    ON _timescaledb_internal.bgw_job_stat_history(execution_start);
CREATE INDEX IF NOT EXISTS bgw_job_stat_history_job_id_execution_start_idx
    ON _timescaledb_internal.bgw_job_stat_history(job_id, execution_start DESC);

DROP INDEX IF EXISTS _timescaledb_internal.bgw_job_stat_history_job_id_idx;


--
-- #9418: Keep bgw_job.proc_schema in sync when a procedure moves schema.
--
-- When ALTER PROCEDURE ... SET SCHEMA is executed, proc_schema in bgw_job
-- is not updated, causing the next scheduler run to fail with
-- "cache lookup failed for function 0".
--
-- The event trigger fires after every ALTER FUNCTION/PROCEDURE command.
-- It finds job rows whose procedure no longer exists at the stored schema
-- (meaning it was moved) and updates proc_schema to the new location.
--

CREATE OR REPLACE FUNCTION _timescaledb_functions.bgw_job_proc_schema_update()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp
AS $$
DECLARE
    obj      record;
    new_nsp  name;
    proc_nm  name;
BEGIN
    FOR obj IN
        SELECT *
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag IN ('ALTER FUNCTION', 'ALTER PROCEDURE')
          AND object_type IN ('function', 'procedure')
    LOOP
        -- Resolve the function's current (new) schema and name by OID.
        SELECT p.proname, n.nspname
        INTO   proc_nm, new_nsp
        FROM   pg_catalog.pg_proc     p
        JOIN   pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE  p.oid = obj.objid;

        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        -- Update any bgw_job row that references this function by name but
        -- in a schema where it no longer exists (i.e. it was moved away).
        UPDATE _timescaledb_config.bgw_job
        SET    proc_schema = new_nsp
        WHERE  proc_name = proc_nm
          AND  proc_schema <> new_nsp
          AND  NOT EXISTS (
                   SELECT 1
                   FROM   pg_catalog.pg_proc     p2
                   JOIN   pg_catalog.pg_namespace n2 ON n2.oid = p2.pronamespace
                   WHERE  p2.proname = proc_nm
                     AND  n2.nspname = proc_schema
               );
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER timescaledb_bgw_job_proc_schema_sync
    ON ddl_command_end
    WHEN TAG IN ('ALTER FUNCTION', 'ALTER PROCEDURE')
    EXECUTE FUNCTION _timescaledb_functions.bgw_job_proc_schema_update();

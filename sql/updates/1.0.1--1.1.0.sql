DROP FUNCTION IF EXISTS _timescaledb_internal.get_version();
DROP FUNCTION IF EXISTS drop_chunks(INTERVAL, NAME, NAME, BOOLEAN);
DROP FUNCTION IF EXISTS drop_chunks(ANYELEMENT, NAME, NAME, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunks_impl(BIGINT, NAME, NAME, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunks_type_check(REGTYPE, NAME, NAME);
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_get_time(INTEGER);
DROP FUNCTION IF EXISTS create_hypertable(regclass, name, name, integer, name, name, anyelement, boolean, boolean, regproc, boolean, text, regproc);
DROP FUNCTION IF EXISTS _timescaledb_internal.to_microseconds(TIMESTAMPTZ);
DROP FUNCTION IF EXISTS _timescaledb_internal.to_timestamp_pg(BIGINT);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_to_internal(anyelement);

--Now we define the argument tables for available BGW policies.
CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_recluster (
    job_id          		INTEGER     PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		INTEGER     UNIQUE NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	hypertable_index_name	NAME		NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_recluster', '');

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_drop_chunks (
    job_id          		INTEGER     PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		INTEGER     UNIQUE NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	older_than				INTERVAL    NOT NULL,
	cascade					BOOLEAN
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_drop_chunks', '');

----- End BGW policy table definitions

-- Now we define a special stats table for each job/chunk pair. This will be used by the scheduler
-- to determine whether to run a specific job on a specific chunk.
CREATE TABLE IF NOT EXISTS _timescaledb_internal.bgw_policy_chunk_stats (
	job_id					INTEGER 	NOT NULL REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
	chunk_id				INTEGER		NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
	num_times_job_run		INTEGER,
	last_time_job_run		TIMESTAMPTZ,
	UNIQUE(job_id,chunk_id)
);

GRANT SELECT ON _timescaledb_config.bgw_policy_recluster TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_policy_drop_chunks TO PUBLIC;
GRANT SELECT ON _timescaledb_internal.bgw_policy_chunk_stats TO PUBLIC;

--Now we define the argument tables for available BGW policies.
CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_reorder (
    job_id          		INTEGER     PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		INTEGER     UNIQUE NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	hypertable_index_name	NAME		NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_reorder', '');

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

GRANT SELECT ON _timescaledb_config.bgw_policy_reorder TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_policy_drop_chunks TO PUBLIC;
GRANT SELECT ON _timescaledb_internal.bgw_policy_chunk_stats TO PUBLIC;

DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunks_impl(REGCLASS, "any", "any", BOOLEAN);
DROP FUNCTION IF EXISTS drop_chunks("any", NAME, NAME, BOOLEAN, "any");

DROP FUNCTION IF EXISTS _timescaledb_internal.get_os_info();

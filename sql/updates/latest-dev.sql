
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.optional_index_info (
    hypertable_index_name NAME PRIMARY KEY,
    is_scheduled BOOLEAN DEFAULT false
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.optional_index_info', '');

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_scheduled_index (
    job_id          		INTEGER     PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		INTEGER     UNIQUE NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	hypertable_index_name	NAME		NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_scheduled_index', '');

-- Add continuous_aggs_jobs_refresh_ranges table
CREATE TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges (
  materialization_id integer NOT NULL,
  start_range bigint NOT NULL,
  end_range bigint NOT NULL,
  pid integer NOT NULL,
  CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_jobs_refresh_ranges', '');

CREATE INDEX continuous_aggs_jobs_refresh_ranges_idx ON _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges (materialization_id);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges TO PUBLIC;

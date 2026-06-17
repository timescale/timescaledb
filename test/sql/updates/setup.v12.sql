-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir setup.v11.sql

CREATE schema user_views;

CREATE OR REPLACE VIEW user_views.hypertables AS SELECT * FROM timescaledb_information.hypertables;
CREATE OR REPLACE VIEW user_views.job_stats AS SELECT * FROM timescaledb_information.job_stats;
CREATE OR REPLACE VIEW user_views.jobs AS SELECT * FROM timescaledb_information.jobs;
CREATE OR REPLACE VIEW user_views.continuous_aggregates AS SELECT * FROM timescaledb_information.continuous_aggregates;
CREATE OR REPLACE VIEW user_views.chunks AS SELECT * FROM timescaledb_information.chunks;
CREATE OR REPLACE VIEW user_views.dimensions AS SELECT * FROM timescaledb_information.dimensions;
CREATE OR REPLACE VIEW user_views.compression_settings AS SELECT * FROM timescaledb_information.compression_settings;
CREATE OR REPLACE VIEW user_views.job_errors AS SELECT * FROM timescaledb_information.job_errors;
CREATE OR REPLACE VIEW user_views.job_history AS SELECT * FROM timescaledb_information.job_history;
CREATE OR REPLACE VIEW user_views.hypertable_compression_settings AS SELECT * FROM timescaledb_information.hypertable_compression_settings;
CREATE OR REPLACE VIEW user_views.chunk_compression_settings AS SELECT * FROM timescaledb_information.chunk_compression_settings;
CREATE OR REPLACE VIEW user_views.hypertable_columnstore_settings AS SELECT * FROM timescaledb_information.hypertable_columnstore_settings;
CREATE OR REPLACE VIEW user_views.chunk_columnstore_settings AS SELECT * FROM timescaledb_information.chunk_columnstore_settings;


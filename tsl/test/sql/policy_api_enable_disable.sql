-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test_table(time timestamptz, chunk_id int);
SELECT create_hypertable('test_table', 'time');
ALTER TABLE test_table SET (timescaledb.compress);

-- reorder

SELECT add_reorder_policy('test_table', 'test_table_time_idx') AS reorder_job_id \gset

SELECT disable_reorder_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=1;

SELECT disable_reorder_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=0;

SELECT scheduled FROM _timescaledb_config.bgw_job WHERE id = :reorder_job_id \gset
SELECT :'scheduled'::bool = false;

SELECT enable_reorder_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=1;

SELECT enable_reorder_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=0;

SELECT scheduled FROM _timescaledb_config.bgw_job WHERE id = :reorder_job_id \gset
SELECT :'scheduled'::bool = true;

SELECT remove_reorder_policy('test_table');

-- retention

SELECT add_retention_policy('test_table', INTERVAL '4 months', true) AS retention_job_id \gset

SELECT disable_retention_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=1;

SELECT disable_retention_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=0;

SELECT scheduled FROM _timescaledb_config.bgw_job WHERE id = :retention_job_id \gset
SELECT :'scheduled'::bool = false;

SELECT enable_retention_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=1;

SELECT enable_retention_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=0;

SELECT scheduled FROM _timescaledb_config.bgw_job WHERE id = :retention_job_id \gset
SELECT :'scheduled'::bool = true;

SELECT remove_retention_policy('test_table');

-- compression

SELECT add_compression_policy('test_table', compress_after => '1 month'::INTERVAL) AS compression_job_id \gset

SELECT disable_compression_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=1;

SELECT disable_compression_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=0;

SELECT scheduled FROM _timescaledb_config.bgw_job WHERE id = :compression_job_id \gset
SELECT :'scheduled'::bool = false;

SELECT enable_compression_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=1;

SELECT enable_compression_policy('test_table') AS affected_policies \gset
SELECT :affected_policies=0;

SELECT scheduled FROM _timescaledb_config.bgw_job WHERE id = :compression_job_id \gset
SELECT :'scheduled'::bool = true;

SELECT remove_compression_policy('test_table');

-- all together now

SELECT add_reorder_policy('test_table', 'test_table_time_idx') AS reorder_job_id \gset
SELECT add_retention_policy('test_table', INTERVAL '4 months', true) AS retention_job_id \gset
SELECT add_compression_policy('test_table', compress_after => '1 month'::INTERVAL) AS compression_job_id \gset

SELECT disable_all_policies('test_table') AS affected_policies \gset
SELECT :affected_policies = 3;

SELECT disable_all_policies('test_table') AS affected_policies \gset
SELECT :affected_policies = 0;

SELECT enable_all_policies('test_table') AS affected_policies \gset
SELECT :affected_policies = 3;

SELECT enable_all_policies('test_table') AS affected_policies \gset
SELECT :affected_policies = 0;

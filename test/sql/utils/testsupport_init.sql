-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT _timescaledb_functions.stop_background_workers();

-- Cleanup any system job stats that can lead to flaky test
DELETE FROM _timescaledb_internal.bgw_job_stat_history WHERE job_id < 1000;
DELETE FROM _timescaledb_internal.bgw_job_stat WHERE job_id < 1000;

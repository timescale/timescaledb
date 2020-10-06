-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT
    *
FROM
    _timescaledb_config.bgw_policy_drop_chunks
ORDER BY
    job_id;

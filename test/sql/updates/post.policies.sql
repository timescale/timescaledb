-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- SELECT * FROM _timescaledb_config.bgw_job WHERE id <> 1 ORDER BY id;
SELECT relnamespace::regnamespace "JOB_SCHEMA" FROM pg_class WHERE relname='bgw_job' and relkind = 'r' \gset
-- hypertable_id shifts across the upgrade (internal compressed hypertables no
-- longer consume ids), so normalize it both as a column and inside the config.
SELECT id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled,
       pg_temp.normalize_hypertable_id(hypertable_id) AS hypertable,
       CASE WHEN config ? 'hypertable_id'
            THEN jsonb_set(config, '{hypertable_id}', to_jsonb(pg_temp.normalize_hypertable_id((config->>'hypertable_id')::int)))
            ELSE config END AS config
FROM :JOB_SCHEMA.bgw_job WHERE id <> 1 ORDER BY id;


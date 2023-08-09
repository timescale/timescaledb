-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE policy_test_timestamptz(time timestamptz not null, device_id int, value float);
SELECT table_name FROM create_hypertable('policy_test_timestamptz','time');

ALTER TABLE policy_test_timestamptz SET (timescaledb.compress);

SELECT
  (string_to_array(extversion,'.'))[1] AS ts_major,
  (string_to_array(extversion,'.'))[2] AS ts_minor
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

SELECT
  :ts_major < 2 AS has_drop_chunks_policy
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN

  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  PERFORM add_reorder_policy('policy_test_timestamptz','policy_test_timestamptz_time_idx');

  -- some policy API functions got renamed for 2.0 so we need to make
  -- sure to use the right name for the version
  IF ts_version < '2.0.0' THEN
    PERFORM add_drop_chunks_policy('policy_test_timestamptz','60d'::interval);
    PERFORM add_compress_chunks_policy('policy_test_timestamptz','10d'::interval);
  ELSE
    PERFORM add_retention_policy('policy_test_timestamptz','60d'::interval);
    PERFORM add_compression_policy('policy_test_timestamptz','10d'::interval);
  END IF;
END
$$;

\if :WITH_ROLES
-- For PostgreSQL 15 and later
GRANT ALL ON SCHEMA PUBLIC TO "dotted.name";
GRANT ALL ON SCHEMA PUBLIC TO "Kim Possible";

SET ROLE "dotted.name";
CREATE TABLE policy_test_user_1(time timestamptz not null, device_id int, value float);
SELECT table_name FROM create_hypertable('policy_test_user_1','time');
\if :has_drop_chunks_policy
SELECT add_drop_chunks_policy('policy_test_user_1', '14 days'::interval);
\else
SELECT add_retention_policy('policy_test_user_1', '14 days'::interval);
\endif

SET ROLE "Kim Possible";
CREATE TABLE policy_test_user_2(time timestamptz not null, device_id int, value float);
SELECT table_name FROM create_hypertable('policy_test_user_2','time');
\if :has_drop_chunks_policy
SELECT add_drop_chunks_policy('policy_test_user_2', '14 days'::interval);
\else
SELECT add_retention_policy('policy_test_user_2', '14 days'::interval);
\endif
RESET ROLE;
\endif

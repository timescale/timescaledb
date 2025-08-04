-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE policy_test_timestamptz(time timestamptz not null, device_id int, value float);
SELECT table_name FROM create_hypertable('policy_test_timestamptz','time');

ALTER TABLE policy_test_timestamptz SET (timescaledb.compress, timescaledb.compress_orderby = '"time" desc');

INSERT INTO policy_test_timestamptz(time, device_id, value) VALUES ('3020-01-01 00:00:00', 1, 1.0);
SELECT compress_chunk(show_chunks('policy_test_timestamptz'));

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_major INT;
  ts_minor INT;
BEGIN

  WITH timescale_version AS (
      SELECT string_to_array(extversion,'.') AS v
      FROM pg_extension
      WHERE extname = 'timescaledb'
  )
  SELECT v[1], v[2]
  INTO ts_major, ts_minor
  FROM timescale_version;

  PERFORM add_reorder_policy('policy_test_timestamptz','policy_test_timestamptz_time_idx');
  PERFORM add_retention_policy('policy_test_timestamptz','60d'::interval);

  -- some policy API functions got renamed for 2.0 so we need to make
  -- sure to use the right name for the version. The schedule_interval
  -- parameter of add_compression_policy was introduced in 2.8.0
  IF ts_major = 2 AND ts_minor < 8 THEN
    PERFORM add_compression_policy('policy_test_timestamptz','10d'::interval);
  ELSE
    PERFORM add_compression_policy('policy_test_timestamptz','10d'::interval, schedule_interval => '3 days 12:00:00'::interval);
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
SELECT add_retention_policy('policy_test_user_1', '14 days'::interval);

SET ROLE "Kim Possible";
CREATE TABLE policy_test_user_2(time timestamptz not null, device_id int, value float);
SELECT table_name FROM create_hypertable('policy_test_user_2','time');
SELECT add_retention_policy('policy_test_user_2', '14 days'::interval);
RESET ROLE;
\endif

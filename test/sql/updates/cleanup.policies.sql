-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN

  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  PERFORM remove_reorder_policy('policy_test_timestamptz');

  -- some policy API functions got renamed for 2.0 so we need to make
  -- sure to use the right name for the version
  IF ts_version < '2.0.0' THEN
    PERFORM remove_drop_chunks_policy('policy_test_timestamptz');
    PERFORM remove_compress_chunks_policy('policy_test_timestamptz');
  ELSE
    PERFORM remove_retention_policy('policy_test_timestamptz');
    PERFORM remove_compression_policy('policy_test_timestamptz');
  END IF;
END
$$;

DROP TABLE policy_test_timestamptz;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  -- Can only run multinode on 2.0.0+
  IF ts_version >= '2.0.0' THEN
    RAISE NOTICE 'creating multinode setup for version % on database %',
		  ts_version, current_database();
    PERFORM add_data_node('dn1', host=>'localhost', database=>'dn1');
	CREATE TABLE disthyper (time timestamptz, device int, temp float);
	PERFORM create_distributed_hypertable('disthyper', 'time', 'device');
	INSERT INTO disthyper VALUES ('2020-12-20 12:18', 1, 27.9);
  END IF;
END
$$;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ECHO errors
\set VERBOSITY default

DO $$
BEGIN
  ASSERT( _timescaledb_internal.get_partition_for_key(''::text) = 669664877 );
  ASSERT( _timescaledb_internal.get_partition_for_key('dev1'::text) = 1129986420 );
  ASSERT( _timescaledb_internal.get_partition_for_key('longlonglonglongpartitionkey'::text) = 1169179734);
END$$;

\unset ECHO
\o /dev/null
\ir include/create_single_db.sql
\o
\set ECHO errors
\set VERBOSITY default

DO $$
BEGIN
  ASSERT( _timescaledb_internal.get_partition_for_key('', 16:: INT4) = 13 );
  ASSERT( _timescaledb_internal.get_partition_for_key('dev1', 16:: INT4) = 4 );
  ASSERT( _timescaledb_internal.get_partition_for_key('longlonglonglongpartitionkey', 16:: INT4) = 6 );
END$$;

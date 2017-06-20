\unset ECHO
\o /dev/null
\ir include/create_single_db.sql
\o
\set ECHO errors
\set VERBOSITY default

DO $$
BEGIN
  ASSERT( _timescaledb_internal.get_partition_for_key('') = 669664877 );
  ASSERT( _timescaledb_internal.get_partition_for_key('dev1') = 1129986420 );
  ASSERT( _timescaledb_internal.get_partition_for_key('longlonglonglongpartitionkey') = 1169179734);
END$$;

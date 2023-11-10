-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ECHO errors
\set VERBOSITY default
\c :TEST_DBNAME :ROLE_SUPERUSER

DO $$
BEGIN
  ASSERT( _timescaledb_functions.get_partition_for_key(''::text) = 669664877 );
  ASSERT( _timescaledb_functions.get_partition_for_key('dev1'::text) = 1129986420 );
  ASSERT( _timescaledb_functions.get_partition_for_key('longlonglonglongpartitionkey'::text) = 1169179734);
END$$;

\pset null '[NULL]'
CREATE USER wizard;
SELECT * FROM (
    VALUES
       (_timescaledb_functions.makeaclitem('wizard', 'wizard', 'insert', false)),
       (_timescaledb_functions.makeaclitem('wizard', 'wizard', 'insert,select', false)),
       (_timescaledb_functions.makeaclitem('wizard', 'wizard', 'insert', true)),
       (_timescaledb_functions.makeaclitem('wizard', 'wizard', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(NULL, 'wizard', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem('wizard', NULL, 'insert,select', true)),
       (_timescaledb_functions.makeaclitem('wizard', 'wizard', NULL, true)),
       (_timescaledb_functions.makeaclitem('wizard', 'wizard', 'insert,select', NULL)),
       (_timescaledb_functions.makeaclitem(0, 'wizard', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem('wizard', 0, 'insert,select', true))
    ) AS t(item);
DROP USER wizard;

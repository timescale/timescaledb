-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ECHO errors
\set VERBOSITY default
\c :TEST_DBNAME :ROLE_SUPERUSER

\set TMP_USER :TEST_DBNAME _wizard

DO $$
BEGIN
  ASSERT( _timescaledb_functions.get_partition_for_key(''::text) = 669664877 );
  ASSERT( _timescaledb_functions.get_partition_for_key('dev1'::text) = 1129986420 );
  ASSERT( _timescaledb_functions.get_partition_for_key('longlonglonglongpartitionkey'::text) = 1169179734);
END$$;

\pset null '[NULL]'
CREATE USER :TMP_USER;
SELECT * FROM (
    VALUES
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert', false)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert,select', false)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(NULL, :'TMP_USER', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', NULL, 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', NULL, true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert,select', NULL)),
       (_timescaledb_functions.makeaclitem(0, :'TMP_USER', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', 0, 'insert,select', true))
    ) AS t(item);
DROP USER :TMP_USER;

SELECT _timescaledb_debug.extension_state();

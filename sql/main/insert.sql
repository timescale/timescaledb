-- This file contains functions that aid in inserting data into a hypertable.

CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_trigger_on_copy_table_c()
 RETURNS TRIGGER AS '$libdir/timescaledb', 'insert_trigger_on_copy_table_c' LANGUAGE C;

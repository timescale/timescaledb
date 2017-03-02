-- This file contains functions that aid in inserting data into a hypertable.

CREATE OR REPLACE FUNCTION _iobeamdb_internal.insert_trigger_on_copy_table_c()
 RETURNS TRIGGER AS '$libdir/iobeamdb', 'insert_trigger_on_copy_table_c' LANGUAGE C;








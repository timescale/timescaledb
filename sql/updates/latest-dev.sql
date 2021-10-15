DROP FUNCTION IF EXISTS _timescaledb_internal.time_col_name_for_chunk(name,name);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_col_type_for_chunk(name,name);

CREATE OR REPLACE FUNCTION _timescaledb_internal.subtract_integer_from_now( hypertable_relid REGCLASS, lag INT8 )
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_subtract_integer_from_now' LANGUAGE C STABLE STRICT;

-- rewrite cagg to ensure selectedCols is set correctly in the view rules --
-- we do this in post-update.sql
-- needed for this feature

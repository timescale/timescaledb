DROP FUNCTION IF EXISTS _timescaledb_internal.time_col_name_for_chunk(name,name);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_col_type_for_chunk(name,name);
-- rewrite cagg to ensure selectedCols is set correctly in the view rules --
-- we do this in post-update.sql
-- needed for this feature

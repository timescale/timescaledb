
CREATE OR REPLACE FUNCTION ddl_is_add_column(pg_ddl_command)
  RETURNS bool IMMUTABLE STRICT
  AS '$libdir/timescaledb' LANGUAGE C;

CREATE OR REPLACE FUNCTION ddl_is_drop_column(pg_ddl_command)
  RETURNS bool IMMUTABLE STRICT
  AS '$libdir/timescaledb' LANGUAGE C;

CREATE OR REPLACE FUNCTION restore_timescaledb()
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    SELECT _timescaledb_internal.setup_meta();
    SELECT _timescaledb_internal.setup_main(true);
$BODY$;

-- Error codes defined in errors.h
-- The functions in this file are just utility commands for throwing common errors.

CREATE OR REPLACE FUNCTION _timescaledb_internal.on_trigger_error(
    tg_op     TEXT,
    tg_schema NAME,
    tg_table  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RAISE EXCEPTION 'Operation % not supported on %.%', tg_op, tg_schema, tg_table
    USING ERRCODE = 'IO101';
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.on_truncate_block()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;


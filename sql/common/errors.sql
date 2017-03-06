-- Defines error codes used
-- PREFIX IO

-- IO000 - GROUP: query errors
-- IO001 - hypertable does not exist
-- IO002 - column does not exist

--IO100 - GROUP: DDL errors
--IO101 - operation not supported
--IO102 - bad hypertable definition
--IO110 - hypertable already exists
--I0120 - node already exists
--I0130 - user already exists
--IO500 - GROUP: internal error
--IO501 - unexpected state/event
--IO502 - communication/remote error

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

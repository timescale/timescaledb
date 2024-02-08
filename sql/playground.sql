-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This function allows users to create playground hypertables with production-like data for feature testing.

CREATE OR REPLACE FUNCTION _timescaledb_functions.create_playground(
    src_hypertable REGCLASS,
    compressed BOOL = false
) RETURNS TEXT AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _src_relation NAME;
    _playground_table NAME;
    _chunk_name NAME;
    _chunk_check BOOL;
    _playground_schema_check BOOL;
    _next_id INTEGER;
    _dimension TEXT;
    _interval TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM information_schema.schemata
    WHERE schema_name = 'tsdb_playground') INTO _playground_schema_check;

    IF NOT _playground_schema_check THEN
        RAISE EXCEPTION '"tsdb_playground" schema must be created before running this';
    END IF;

    -- get schema and table name
    SELECT n.nspname, c.relname INTO _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    INNER JOIN timescaledb_information.hypertables i ON (i.hypertable_name = c.relname )
    WHERE c.oid = src_hypertable;

    IF _table_name IS NULL THEN
        RAISE EXCEPTION '% is not a hypertable', src_hypertable;
    END IF;

    SELECT EXISTS(SELECT 1 FROM timescaledb_information.chunks WHERE hypertable_name = _table_name AND hypertable_schema = _schema_name) INTO _chunk_check;

    IF NOT _chunk_check THEN
        RAISE EXCEPTION '% has no chunks for playground testing', src_hypertable;
    END IF;

    EXECUTE pg_catalog.format($$ CREATE SEQUENCE IF NOT EXISTS tsdb_playground.%I $$, _table_name||'_seq');
    SELECT pg_catalog.nextval('tsdb_playground.' || pg_catalog.quote_ident(_table_name || '_seq')) INTO _next_id;

    SELECT pg_catalog.format('%I.%I', _schema_name, _table_name) INTO _src_relation;

    SELECT pg_catalog.format('tsdb_playground.%I', _table_name || '_' || _next_id::text) INTO _playground_table;
    EXECUTE pg_catalog.format(
        $$ CREATE TABLE %s (like %s including comments including constraints including defaults including indexes) $$
        , _playground_table, _src_relation
        );

    -- get dimension column from src ht for partitioning playground ht
    SELECT column_name, time_interval INTO _dimension, _interval FROM timescaledb_information.dimensions WHERE hypertable_name = _table_name AND hypertable_schema = _schema_name LIMIT 1;

    PERFORM public.create_hypertable(_playground_table::REGCLASS, _dimension::NAME, chunk_time_interval := _interval::interval);

    -- Ideally, it should pick up the latest complete chunk (second last chunk) from this hypertable.
    -- If num_chunks > 1 then it will get true, converted into 1, taking the second row, otherwise it'll get false converted to 0 and get no offset.
    SELECT
        format('%I.%I',chunk_schema,chunk_name)
    INTO STRICT
        _chunk_name
    FROM
        timescaledb_information.chunks
    WHERE
        hypertable_schema = _schema_name AND
        hypertable_name = _table_name
    ORDER BY
        chunk_creation_time DESC OFFSET (
            SELECT
                (num_chunks > 1)::integer
            FROM timescaledb_information.hypertables
            WHERE
                hypertable_name = _table_name)
    LIMIT 1;
	EXECUTE pg_catalog.format($$ INSERT INTO %s SELECT * FROM %s $$, _playground_table, _chunk_name);

    -- ToDo,
    --if compressed:
    -- --retrieve compression settings
    -- --compress
    --retrieve space dimension if there is any, otherwise get time one
	RETURN _playground_table;
END
$BODY$
LANGUAGE PLPGSQL SET search_path TO pg_catalog, pg_temp;

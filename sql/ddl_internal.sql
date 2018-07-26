-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

-- show_chunks for internal use only. The only difference
-- from user-facing API is that this one takes a 4th argument
-- specifying the caller name. This makes it easier to taylor
-- error messages to the caller function context.
CREATE OR REPLACE FUNCTION _timescaledb_internal.show_chunks_impl(
    hypertable  REGCLASS = NULL,
    older_than "any" = NULL,
    newer_than "any" = NULL,
    caller_name NAME = NULL
) RETURNS SETOF REGCLASS AS '@MODULE_PATHNAME@', 'ts_chunk_show_chunks'
LANGUAGE C STABLE PARALLEL SAFE;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table. Any of the first three arguments
-- can be NULL meaning "all values".
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_impl(
    older_than_time  ANYELEMENT = NULL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    newer_than_time ANYELEMENT = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row REGCLASS;
    cascade_mod TEXT = '';
    exist_count INT = 0;
BEGIN

    IF cascade THEN
        cascade_mod = 'CASCADE';
    END IF;

    IF table_name IS NOT NULL THEN
        SELECT COUNT(*)
        FROM _timescaledb_catalog.hypertable h
        WHERE (drop_chunks_impl.schema_name IS NULL OR h.schema_name = drop_chunks_impl.schema_name)
        AND drop_chunks_impl.table_name = h.table_name
        INTO STRICT exist_count;

        IF exist_count = 0 THEN
            RAISE 'hypertable "%" does not exist', drop_chunks_impl.table_name
            USING ERRCODE = 'TS001';
        END IF;
    END IF;

    FOR schema_name, table_name IN
        SELECT hyper.schema_name, hyper.table_name
        FROM _timescaledb_catalog.hypertable hyper
        WHERE
            (drop_chunks_impl.schema_name IS NULL OR hyper.schema_name = drop_chunks_impl.schema_name) AND
            (drop_chunks_impl.table_name IS NULL OR hyper.table_name = drop_chunks_impl.table_name)
    LOOP
        FOR chunk_row IN SELECT _timescaledb_internal.show_chunks_impl(schema_name || '.' || table_name, older_than_time, newer_than_time, 'drop_chunks')
        LOOP
            EXECUTE format(
                    $$
                    DROP TABLE %s %s
                    $$, chunk_row, cascade_mod
            );
        END LOOP;
    END LOOP;
END
$BODY$;

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;

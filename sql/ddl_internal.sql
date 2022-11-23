-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_replica_table(
    chunk REGCLASS,
    data_node_name NAME
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_chunk_create_replica_table' LANGUAGE C VOLATILE;

-- Drop the specified chunk replica on the specified data node
CREATE OR REPLACE FUNCTION  _timescaledb_internal.chunk_drop_replica(
    chunk                   REGCLASS,
    node_name               NAME
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_drop_replica' LANGUAGE C VOLATILE;

CREATE OR REPLACE PROCEDURE _timescaledb_internal.wait_subscription_sync(
    schema_name    NAME,
    table_name     NAME,
    retry_count    INT DEFAULT 18000,
    retry_delay_ms NUMERIC DEFAULT 0.200
)
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    in_sync BOOLEAN;
BEGIN
    FOR i in 1 .. retry_count
    LOOP
        SELECT pgs.srsubstate = 'r'
        INTO in_sync
        FROM pg_subscription_rel pgs
        JOIN pg_class pgc ON relname = table_name
        JOIN pg_namespace n ON (n.OID = pgc.relnamespace)
        WHERE pgs.srrelid = pgc.oid AND schema_name = n.nspname;

        if (in_sync IS NULL OR NOT in_sync) THEN
          PERFORM pg_sleep(retry_delay_ms);
        ELSE
          RETURN;
        END IF;
    END LOOP;
    RAISE 'subscription sync wait timedout';
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.health() RETURNS
TABLE (node_name NAME, healthy BOOL, in_recovery BOOL, error TEXT)
AS '@MODULE_PATHNAME@', 'ts_health_check' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_stale_chunks(
    node_name NAME,
    chunks integer[] = NULL
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunks_drop_stale' LANGUAGE C VOLATILE;

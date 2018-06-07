-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE FUNCTION _timescaledb_internal.remote_txn_heal_server(foreign_server_oid oid)
RETURNS INT
AS '@MODULE_PATHNAME@', 'ts_remote_txn_heal_server'
LANGUAGE C STRICT;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.
CREATE OR REPLACE FUNCTION _timescaledb_internal.set_dist_id(dist_id UUID) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_set_id' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.remove_from_dist_db() RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_remove_id' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.set_peer_dist_id(dist_id UUID) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_set_peer_id' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION server_hypertable_info(
    server_name              NAME
)
RETURNS TABLE (table_schema    name,
    table_name      name,
    table_owner     name,
    num_dimensions  smallint,
    num_chunks      bigint,
    distributed     bool,
    table_bytes     bigint,
    index_bytes     bigint,
    toast_bytes     bigint,
    total_bytes     bigint)
AS '@MODULE_PATHNAME@', 'ts_dist_remote_hypertable_info' LANGUAGE C VOLATILE STRICT;

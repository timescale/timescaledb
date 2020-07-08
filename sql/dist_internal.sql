-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.set_dist_id(dist_id UUID) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_set_id' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.set_peer_dist_id(dist_id UUID) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_set_peer_id' LANGUAGE C VOLATILE STRICT;

-- Function to validate that a node has local settings to function as
-- a data node. Throws error if validation fails.
CREATE OR REPLACE FUNCTION _timescaledb_internal.validate_as_data_node() RETURNS void
AS '@MODULE_PATHNAME@', 'ts_dist_validate_as_data_node' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.show_connection_cache()
RETURNS TABLE (
    node_name           name,
    user_name           name,
    host                text,
    port                int,
    database            name,
    backend_pid         int,
    connection_status   text,
    transaction_status  text,
    transaction_depth   int,
    processing          boolean,
    invalidated         boolean)
AS '@MODULE_PATHNAME@', 'ts_remote_connection_cache_show' LANGUAGE C VOLATILE STRICT;

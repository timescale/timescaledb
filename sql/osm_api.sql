-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This function updates the dimension slice range stored in the catalog with the min and max
-- values that the OSM chunk contains. Since there is only one OSM chunk per hypertable with
-- only a time dimension, the hypertable is used to determine the corresponding slice
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_osm_range_update(
    hypertable REGCLASS,
    range_start ANYELEMENT = NULL::bigint,
    range_end ANYELEMENT = NULL,
    empty BOOL = false
) RETURNS BOOL AS '@MODULE_PATHNAME@',
'ts_hypertable_osm_range_update' LANGUAGE C VOLATILE;

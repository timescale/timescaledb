-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE FUNCTION _timescaledb_internal.segment_meta_min_max_get_min(_timescaledb_internal.segment_meta_min_max, ANYELEMENT)
   RETURNS ANYELEMENT
   AS '@MODULE_PATHNAME@', 'ts_segment_meta_min_max_get_min'
   LANGUAGE C IMMUTABLE;

CREATE FUNCTION _timescaledb_internal.segment_meta_min_max_get_max(_timescaledb_internal.segment_meta_min_max, ANYELEMENT)
   RETURNS ANYELEMENT
   AS '@MODULE_PATHNAME@', 'ts_segment_meta_min_max_get_max'
   LANGUAGE C IMMUTABLE;

CREATE FUNCTION _timescaledb_internal.segment_meta_min_max_has_null(_timescaledb_internal.segment_meta_min_max)
   RETURNS boolean
   AS '@MODULE_PATHNAME@', 'ts_segment_meta_min_max_has_null'
   LANGUAGE C IMMUTABLE;

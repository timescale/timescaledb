-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- The general compressed_data type;
--
CREATE TYPE _timescaledb_internal.compressed_data;

--
-- Remote transaction ID
--
CREATE TYPE @extschema@.rxid;

--placeholder to allow creation of functions below

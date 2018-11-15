-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

CREATE SCHEMA IF NOT EXISTS _timescaledb_catalog;
CREATE SCHEMA IF NOT EXISTS _timescaledb_internal;
CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
CREATE SCHEMA IF NOT EXISTS _timescaledb_config;
GRANT USAGE ON SCHEMA _timescaledb_cache, _timescaledb_catalog, _timescaledb_internal, _timescaledb_config TO PUBLIC;

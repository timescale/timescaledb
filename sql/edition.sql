-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

CREATE OR REPLACE FUNCTION _timescaledb_internal.enterprise_enabled() RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_enterprise_enabled' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.current_license_key() RETURNS TEXT
AS '@MODULE_PATHNAME@', 'ts_current_license_key' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_loaded() RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_tsl_loaded' LANGUAGE C STABLE;

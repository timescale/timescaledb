-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.enterprise_enabled() RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_enterprise_enabled' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.current_license_key() RETURNS TEXT
AS '@MODULE_PATHNAME@', 'ts_current_license_key' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_loaded() RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_tsl_loaded' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.license_expiration_time() RETURNS TIMESTAMPTZ
AS '@MODULE_PATHNAME@', 'ts_license_expiration_time' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.print_license_expiration_info() RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_print_tsl_license_expiration_info' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.license_edition() RETURNS TEXT
AS '@MODULE_PATHNAME@', 'ts_license_edition' LANGUAGE C;

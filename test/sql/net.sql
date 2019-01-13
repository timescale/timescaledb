-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_http_parsing(int) RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_http_parsing' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_http_parsing_full() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_http_parsing_full' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_http_request_build() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_http_request_build' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_conn() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_conn' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT _timescaledb_internal.test_http_parsing(10000);
SELECT _timescaledb_internal.test_http_parsing_full();
SELECT _timescaledb_internal.test_http_request_build();
SELECT _timescaledb_internal.test_conn();

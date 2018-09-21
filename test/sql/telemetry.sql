\c single :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status(int) RETURNS JSONB
    AS :MODULE_PATHNAME, 'test_status' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status_ssl(int) RETURNS JSONB
    AS :MODULE_PATHNAME, 'test_status_ssl' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status_mock(text) RETURNS JSONB
    AS :MODULE_PATHNAME, 'test_status_mock' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_telemetry_parse_version(response text, installed_major int, installed_minor int, installed_patch int, installed_modtag text = NULL)
    RETURNS TABLE(version_string text, major int, minor int, patch int, modtag text, up_to_date bool)
    AS :MODULE_PATHNAME, 'test_telemetry_parse_version' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_telemetry_main_conn(text, text, text)
RETURNS VOID AS :MODULE_PATHNAME, 'test_telemetry_main_conn' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_telemetry(host text = NULL, servname text = NULL, port int = NULL) RETURNS JSONB AS :MODULE_PATHNAME, 'test_telemetry' LANGUAGE C IMMUTABLE PARALLEL SAFE;

\c single :ROLE_DEFAULT_PERM_USER
SELECT _timescaledb_internal.test_status_ssl(200);
SELECT _timescaledb_internal.test_status_ssl(201);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.test_status_ssl(304);
SELECT _timescaledb_internal.test_status_ssl(400);
SELECT _timescaledb_internal.test_status_ssl(401);
SELECT _timescaledb_internal.test_status_ssl(404);
SELECT _timescaledb_internal.test_status_ssl(500);
SELECT _timescaledb_internal.test_status_ssl(503);
\set ON_ERROR_STOP 1
SELECT _timescaledb_internal.test_status(200);
SELECT _timescaledb_internal.test_status(201);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.test_status(304);
SELECT _timescaledb_internal.test_status(400);
SELECT _timescaledb_internal.test_status(401);
SELECT _timescaledb_internal.test_status(404);
SELECT _timescaledb_internal.test_status(500);
SELECT _timescaledb_internal.test_status(503);
\set ON_ERROR_STOP 1

-- This function runs the test 5 times, because each time the internal C function is choosing a random length to send from the server on each socket read. We hit many cases this way.
CREATE OR REPLACE FUNCTION mocker(TEXT) RETURNS SETOF TEXT AS
$BODY$
DECLARE
r TEXT;
BEGIN
FOR i in 1..5 LOOP
SELECT _timescaledb_internal.test_status_mock($1) INTO r;
RETURN NEXT r;
END LOOP;
RETURN;
END
$BODY$
LANGUAGE 'plpgsql';

select * from mocker(
        E'HTTP/1.1 200 OK\r\n'
        'Content-Type: application/json; charset=utf-8\r\n'
        'Date: Thu, 12 Jul 2018 18:33:04 GMT\r\n'
        'ETag: W/\"e-upYEWCL+q6R/++2nWHz5b76hBgo\"\r\n'
        'Server: nginx\r\n'
        'Vary: Accept-Encoding\r\n'
        'Content-Length: 14\r\n'
        'Connection: Close\r\n\r\n'
        '{\"status\":200}');
select * from mocker(
        E'HTTP/1.1 201 OK\r\n'
        'Content-Type: application/json; charset=utf-8\r\n'
        'Vary: Accept-Encoding\r\n'
        'Content-Length: 14\r\n'
        'Connection: Close\r\n\r\n'
        '{\"status\":201}');

\set ON_ERROR_STOP 0
\set test_string 'HTTP/1.1 404 Not Found\r\nContent-Length: 14\r\nConnection: Close\r\n\r\n{\"status\":404}';
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
\set test_string 'Content-Length: 14\r\nConnection: Close\r\n\r\n{\"status\":404}';
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
\set test_string 'HTTP/1.1 404 Not Found\r\nContent-Length: 14\r\nConnection: Close\r\n{\"status\":404}';
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
\set ON_ERROR_STOP 1

-- Test parsing version response
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"status": "200", "current_timescaledb_version": "10.1.0"}', 10, 1, 0);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "10.1"}', 10, 1, 0);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "10"}', 10, 1, 0);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "10"}', 9, 1, 1);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "9.2.0"}', 9, 1, 1);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "9.1.2"}', 9, 1, 1);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0"}', 1, 0, 0, 'rc1');

SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-rc1"}', 1, 0, 0, 'rc1');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-rc1"}', 1, 0, 0, 'rc2');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-rc2"}', 1, 0, 0, 'rc1');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-rc1"}', 1, 0, 0, 'alpha');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-alpha"}', 1, 0, 0, 'rc1');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-rc1"}', 1, 0, 1, 'rc1');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-rc1"}', 1, 0, 0, 'beta');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-alpha"}', 1, 0, 0, 'beta');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-alpha1"}', 1, 0, 0, 'alpha2');

\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": " 10 "}', 10, 1, 0);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "a"}', 9, 1, 1);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "a.b.c"}', 9, 1, 1);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "10.1.1a"}', 9, 1, 1);
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "10.1.1+rc1"}', 10, 1, 1, 'rc1');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "10.1.1.1"}', 10, 1, 1, 'rc1');
SELECT * FROM _timescaledb_internal.test_telemetry_parse_version('{"current_timescaledb_version": "1.0.0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}', 1, 0, 0, 'alpha2');
\set ON_ERROR_STOP 1
SET timescaledb.telemetry_level=basic;
SELECT _timescaledb_internal.test_telemetry_main_conn('timescale.com', 'path', 'http');
SELECT _timescaledb_internal.test_telemetry_main_conn('timescale.com', 'path', 'https');
SELECT _timescaledb_internal.test_telemetry_main_conn('telemetry.timescale.com', 'path', 'http');
SET timescaledb.telemetry_level=off;

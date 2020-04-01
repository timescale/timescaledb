-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status(int) RETURNS JSONB
    AS :MODULE_PATHNAME, 'ts_test_status' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status_ssl(int) RETURNS JSONB
    AS :MODULE_PATHNAME, 'ts_test_status_ssl' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status_mock(text) RETURNS JSONB
    AS :MODULE_PATHNAME, 'ts_test_status_mock' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_validate_server_version(response text)
    RETURNS TEXT
    AS :MODULE_PATHNAME, 'ts_test_validate_server_version' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_telemetry_main_conn(text, text)
RETURNS BOOLEAN AS :MODULE_PATHNAME, 'ts_test_telemetry_main_conn' LANGUAGE C IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_telemetry(host text = NULL, servname text = NULL, port int = NULL) RETURNS JSONB AS :MODULE_PATHNAME, 'ts_test_telemetry' LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION test_check_version_response(response text)
       RETURNS VOID
       AS :MODULE_PATHNAME, 'ts_test_check_version_response'
       LANGUAGE C
       IMMUTABLE STRICT PARALLEL SAFE;
       
INSERT INTO _timescaledb_catalog.metadata VALUES ('foo','bar',TRUE);
INSERT INTO _timescaledb_catalog.metadata VALUES ('bar','baz',FALSE);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
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
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"status": "200", "current_timescaledb_version": "10.1.0"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "10.1"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "10"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "9.2.0"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "9.1.2"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "1.0.0"}');

SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "1.0.0-rc1"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "1.0.0-rc2"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "1.0.0-rc1"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "1.0.0-alpha"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "123456789"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "!@#$%"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": ""}');

SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": " 10 "}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "a"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "a.b.c"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "10.1.1a"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "10.1.1+rc1"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "10.1.1.1"}');
SELECT * FROM _timescaledb_internal.test_validate_server_version('{"current_timescaledb_version": "1.0.0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}');


----------------------------------------------------------------
-- Test well-formed response and valid versions
SELECT test_check_version_response('{"current_timescaledb_version": "1.6.1", "is_up_to_date": true}');
SELECT test_check_version_response('{"current_timescaledb_version": "1.6.1", "is_up_to_date": false}');
SELECT test_check_version_response('{"current_timescaledb_version": "10.1", "is_up_to_date": false}');
SELECT test_check_version_response('{"current_timescaledb_version": "10.1.1-rc1", "is_up_to_date": false}');

----------------------------------------------------------------
-- Test well-formed response but invalid versions
SELECT test_check_version_response('{"current_timescaledb_version": "1.0.0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "is_up_to_date": false}');
SELECT test_check_version_response('{"current_timescaledb_version": "10.1.1+rc1", "is_up_to_date": false}');
SELECT test_check_version_response('{"current_timescaledb_version": "@10.1.1", "is_up_to_date": false}');
SELECT test_check_version_response('{"current_timescaledb_version": "10.1.1@", "is_up_to_date": false}');

----------------------------------------------------------------
-- Test malformed responses

\set ON_ERROR_STOP 0
-- Empty response
SELECT test_check_version_response('{}');

-- Field "is_up_to_date" missing
SELECT test_check_version_response('{"current_timescaledb_version": "1.6.1"}');

-- Field "current_timescaledb_version" is missing
SELECT test_check_version_response('{"is_up_to_date": false}');
\set ON_ERROR_STOP 1

SET timescaledb.telemetry_level=basic;
-- Connect to a bogus host and path to test error handling in telemetry_main()
SELECT _timescaledb_internal.test_telemetry_main_conn('noservice.timescale.com', 'path');

-- Test error message and override for when telemetry is disabled
SET timescaledb.telemetry_level=off;
SELECT get_telemetry_report();
SELECT get_telemetry_report(NULL);
SELECT * FROM json_object_keys(get_telemetry_report(always_display_report := true)::json) AS key
WHERE key != 'os_name_pretty';

-- Test telemetry report contents
SET timescaledb.telemetry_level=basic;

SELECT * FROM json_object_keys(get_telemetry_report()::json) AS key
WHERE key != 'os_name_pretty';

-- check telemetry picks up flagged content from metadata
SELECT json_object_field(get_telemetry_report()::json,'db_metadata');

-- check timescaledb_telemetry.cloud
SELECT json_object_field(get_telemetry_report()::json,'instance_metadata');

SELECT json_object_field(get_telemetry_report()::json,'num_hypertables');

SELECT json_object_field(get_telemetry_report()::json,'num_continuous_aggs');

CREATE TABLE device_readings (
      observation_time  TIMESTAMPTZ       NOT NULL
);
SELECT table_name FROM create_hypertable('device_readings', 'observation_time');

SELECT json_object_field(get_telemetry_report()::json,'num_hypertables');

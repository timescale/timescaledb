\c single :ROLE_SUPERUSER

DROP EXTENSION timescaledb;
--no extension
\dx
SELECT 1;

CREATE EXTENSION timescaledb VERSION 'mock-1';
SELECT 1;
\dx

CREATE EXTENSION IF NOT EXISTS timescaledb VERSION 'mock-1';
CREATE EXTENSION IF NOT EXISTS timescaledb VERSION 'mock-2';

DROP EXTENSION timescaledb;
\set ON_ERROR_STOP 0
--test that we cannot accidentally load another library version
CREATE EXTENSION IF NOT EXISTS timescaledb VERSION 'mock-2';
\set ON_ERROR_STOP 1

\c single :ROLE_SUPERUSER
--no extension
\dx
SELECT 1;

CREATE EXTENSION timescaledb VERSION 'mock-1';
--same backend as create extension;
SELECT 1;
\dx

--start new backend;
\c single :ROLE_DEFAULT_PERM_USER

SELECT 1;
SELECT 1;
--test fn call after load
SELECT mock_function();
\dx

\c single :ROLE_DEFAULT_PERM_USER
--test fn call as first command
SELECT mock_function();

--use guc to prevent loading
\c single :ROLE_SUPERUSER
SET timescaledb.disable_load = 'on';
SELECT 1;
SELECT 1;
SET timescaledb.disable_load = 'off';
SELECT 1;
\set ON_ERROR_STOP 0
SET timescaledb.disable_load = 'not bool';
\set ON_ERROR_STOP 1


\set ON_ERROR_STOP 0
--cannot update extension after .so of previous version already loaded
ALTER EXTENSION timescaledb UPDATE TO 'mock-2';
\set ON_ERROR_STOP 1

\c single_2 :ROLE_SUPERUSER
\dx
CREATE EXTENSION timescaledb VERSION 'mock-1';
\dx
--start a new backend to update
\c single_2 :ROLE_SUPERUSER
ALTER EXTENSION timescaledb UPDATE TO 'mock-2';
SELECT 1;
\dx

--drop extension
DROP EXTENSION timescaledb;
SELECT 1;
\dx


\c single_2 :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-2';
SELECT 1;
\dx

--single still has old version
\c single :ROLE_SUPERUSER
SELECT 1;
\dx

--try a broken upgrade
\c single_2 :ROLE_SUPERUSER
\dx
\set ON_ERROR_STOP 0
ALTER EXTENSION timescaledb UPDATE TO 'mock-3';
\set ON_ERROR_STOP 1
--should still be on mock-2
SELECT 1;
\dx

--drop extension
DROP EXTENSION timescaledb;
SELECT 1;
\dx

--create extension anew, only upgrade was broken
\c single_2 :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-3';
SELECT 1;
\dx
DROP EXTENSION timescaledb;
SELECT 1;

--mismatched version errors
\c single_2 :ROLE_SUPERUSER
\set ON_ERROR_STOP 0
--mock-4 has mismatched versions, so the .so load should throw an error
CREATE EXTENSION timescaledb VERSION 'mock-4';
\set ON_ERROR_STOP 1
--mock-4 not installed.
\dx

\set ON_ERROR_STOP 0
--should not allow since the errored-out mock-4 above already poisoned the well.
CREATE EXTENSION timescaledb VERSION 'mock-5';
\set ON_ERROR_STOP 1

\c single_2 :ROLE_SUPERUSER
--broken version and drop
CREATE EXTENSION timescaledb VERSION 'mock-broken';

\set ON_ERROR_STOP 0
--intentional broken version
\dx
SELECT 1;
SELECT 1;
--cannot drop extension; already loaded broken version
DROP EXTENSION timescaledb;
\set ON_ERROR_STOP 1

\c single_2 :ROLE_SUPERUSER
--can drop extension now. Since drop first command.
DROP EXTENSION timescaledb;
\dx

--broken version and update to fixed
\c single_2 :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-broken';
\set ON_ERROR_STOP 0
--intentional broken version
SELECT 1;
--cannot update extension; already loaded bad version
ALTER EXTENSION timescaledb UPDATE TO 'mock-5';
\set ON_ERROR_STOP 1

\c single_2 :ROLE_SUPERUSER
--can update extension now.
ALTER EXTENSION timescaledb UPDATE TO 'mock-5';
SELECT 1;
SELECT mock_function();

\c single_2 :ROLE_SUPERUSER
ALTER EXTENSION timescaledb UPDATE TO 'mock-6';
\set ON_ERROR_STOP 0
--The mock-5->mock_6 upgrade is intentionally broken.
--The mock_function was never changed to point to mock-6 in the update script.
--Thus mock_function is defined incorrectly to point to the mock-5.so
--This should be an error.
SELECT mock_function();
\set ON_ERROR_STOP 1
\dx

--TEST: create extension when old .so already loaded
\c single :ROLE_SUPERUSER
--force load of extension with (\dx)
\dx
DROP EXTENSION timescaledb;
\dx

\set ON_ERROR_STOP 0
CREATE EXTENSION timescaledb VERSION 'mock-2';
\set ON_ERROR_STOP 1
\dx
--can create in a new session.
\c single :ROLE_SUPERUSER
CREATE EXTENSION timescaledb VERSION 'mock-2';
\dx

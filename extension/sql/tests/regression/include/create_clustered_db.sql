SET client_min_messages = WARNING;
DROP DATABASE IF EXISTS meta;
DROP DATABASE IF EXISTS "Test1";
DROP DATABASE IF EXISTS test2;
SET client_min_messages = NOTICE;

CREATE DATABASE meta;
CREATE DATABASE "Test1";
CREATE DATABASE test2;

\c meta
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
select setup_meta();

\c Test1
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
select setup_main();

\c test2
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
select setup_main();

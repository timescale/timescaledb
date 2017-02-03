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
select set_meta('localhost');

\c Test1
CREATE SCHEMA io_test;
CREATE EXTENSION IF NOT EXISTS iobeamdb SCHEMA io_test CASCADE;
ALTER DATABASE "Test1" SET search_path = "io_test";
SET search_path = 'io_test';
select join_cluster(meta_database => 'meta', meta_hostname => 'localhost', node_hostname => 'localhost');

\c test2
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
select join_cluster(meta_database => 'meta', meta_hostname => 'localhost', node_hostname => 'localhost');

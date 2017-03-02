\set VERBOSITY default
SET client_min_messages = WARNING;
DROP DATABASE IF EXISTS single;
SET client_min_messages = NOTICE;
CREATE DATABASE single;

\c single
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
SELECT setup_single_node(hostname => 'fakehost'); -- fakehost makes sure there is no network connection
\set VERBOSITY verbose

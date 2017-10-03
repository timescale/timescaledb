SET client_min_messages = WARNING;
DROP DATABASE IF EXISTS single;
SET client_min_messages = NOTICE;
CREATE DATABASE single;

\c single
CREATE EXTENSION IF NOT EXISTS timescaledb;

\ir switch_regular_user.sql
SET timescaledb.disable_optimizations = :DISABLE_OPTIMIZATIONS;

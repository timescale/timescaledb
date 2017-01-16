DROP DATABASE IF EXISTS single;
CREATE DATABASE single;

\c single
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
SELECT setup_meta();
SELECT setup_main();


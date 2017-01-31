CREATE SCHEMA IF NOT EXISTS _iobeamdb_catalog;

CREATE TYPE _iobeamdb_catalog.chunk_placement_type AS ENUM ('RANDOM', 'STICKY');


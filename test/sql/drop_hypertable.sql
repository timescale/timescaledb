SELECT * from _timescaledb_catalog.hypertable;
SELECT * from _timescaledb_catalog.dimension;

CREATE TABLE should_drop (time timestamp, temp float8);
SELECT create_hypertable('should_drop', 'time');

CREATE TABLE hyper_with_dependencies (time timestamp, temp float8);
SELECT create_hypertable('hyper_with_dependencies', 'time');

CREATE VIEW dependent_view AS SELECT * FROM hyper_with_dependencies;

INSERT INTO hyper_with_dependencies VALUES (now(), 1.0);

\set ON_ERROR_STOP 0
DROP TABLE hyper_with_dependencies;
\set ON_ERROR_STOP 1
DROP TABLE hyper_with_dependencies CASCADE;
\dv

CREATE TABLE chunk_with_dependencies (time timestamp, temp float8);
SELECT create_hypertable('chunk_with_dependencies', 'time');

INSERT INTO chunk_with_dependencies VALUES (now(), 1.0);

CREATE VIEW dependent_view_chunk AS SELECT * FROM _timescaledb_internal._hyper_3_2_chunk;

\set ON_ERROR_STOP 0
DROP TABLE chunk_with_dependencies;
\set ON_ERROR_STOP 1
DROP TABLE chunk_with_dependencies CASCADE;
\dv

-- Calling create hypertable again will increment hypertable ID
-- although no new hypertable is created. Make sure we can handle this.
SELECT create_hypertable('should_drop', 'time', if_not_exists => true);
SELECT * from _timescaledb_catalog.hypertable;
SELECT * from _timescaledb_catalog.dimension;
DROP TABLE should_drop;

CREATE TABLE should_drop (time timestamp, temp float8);
SELECT create_hypertable('should_drop', 'time');

INSERT INTO should_drop VALUES (now(), 1.0);
SELECT * from _timescaledb_catalog.hypertable;
SELECT * from _timescaledb_catalog.dimension;


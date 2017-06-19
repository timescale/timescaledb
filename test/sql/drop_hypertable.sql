\ir include/create_single_db.sql

SELECT * from _timescaledb_catalog.hypertable;
SELECT * from _timescaledb_catalog.dimension;

CREATE TABLE should_drop (time timestamp, temp float8);
SELECT create_hypertable('should_drop', 'time');

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


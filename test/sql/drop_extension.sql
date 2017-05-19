\o /dev/null
\ir include/create_single_db.sql
\o

CREATE TABLE drop_test(time timestamp, temp float8, device text);

SELECT create_hypertable('drop_test', 'time', 'device', 2);
SELECT * FROM _timescaledb_catalog.hypertable;
INSERT INTO drop_test VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
SELECT * FROM drop_test;

DROP EXTENSION timescaledb CASCADE;

-- Querying the original table should not return any rows since all of
-- them actually existed in chunks that are now gone
SELECT * FROM drop_test;

-- Recreate the extension
CREATE EXTENSION timescaledb CASCADE;

-- Test that calling twice generates proper error
\set ON_ERROR_STOP 0
CREATE EXTENSION timescaledb CASCADE;
\set ON_ERROR_STOP 1

-- Make the table a hypertable again
SELECT create_hypertable('drop_test', 'time', 'device', 2);

SELECT * FROM _timescaledb_catalog.hypertable;
INSERT INTO drop_test VALUES('Mon Mar 20 09:18:19.100462 2017', 22.1, 'dev1');
SELECT * FROM drop_test;

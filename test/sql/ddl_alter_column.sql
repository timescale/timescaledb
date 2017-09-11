\ir include/create_single_db.sql
\ir include/switch_regular_user.sql

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE alter_test(time timestamptz, temp float, color varchar(10));

-- create hypertable with two chunks
SELECT create_hypertable('alter_test', 'time', 'color', 2, chunk_time_interval => 2628000000000);

INSERT INTO alter_test VALUES ('2017-01-20T09:00:01', 17.5, 'blue'),
                              ('2017-01-21T09:00:01', 19.1, 'yellow'),
                              ('2017-04-20T09:00:01', 89.5, 'green'),
                              ('2017-04-21T09:00:01', 17.1, 'black');
\d+ alter_test
\d+ _timescaledb_internal.*
-- show the column name and type of the partitioning dimension in the
-- metadata table
SELECT * FROM _timescaledb_catalog.dimension;

EXPLAIN (costs off)
SELECT * FROM alter_test WHERE time > '2017-05-20T10:00:01';

-- rename column and change its type
ALTER TABLE alter_test RENAME COLUMN time TO time_us;
ALTER TABLE alter_test ALTER COLUMN time_us TYPE timestamp;
ALTER TABLE alter_test RENAME COLUMN color TO colorname;
ALTER TABLE alter_test ALTER COLUMN colorname TYPE text;

\d+ alter_test
\d+ _timescaledb_internal.*
-- show that the metadata has been updated
SELECT * FROM _timescaledb_catalog.dimension;

-- constraint exclusion should still work with updated column
EXPLAIN (costs off)
SELECT * FROM alter_test WHERE time_us > '2017-05-20T10:00:01';

\set ON_ERROR_STOP 0
-- verify that we cannot change the column type to something incompatible
ALTER TABLE alter_test ALTER COLUMN colorname TYPE varchar(3);
-- conversion that messes up partitioning fails
ALTER TABLE alter_test ALTER COLUMN time_us TYPE timestamptz USING time_us::timestamptz+INTERVAL '1 year';
\set ON_ERROR_STOP 1

\ir include/insert_two_partitions.sql

\d+ "_timescaledb_internal".*
SELECT * FROM _timescaledb_catalog.chunk;

SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id;
SELECT * FROM ONLY "two_Partitions";

CREATE TABLE error_test(time timestamp, temp float8, device text NOT NULL);
SELECT create_hypertable('error_test', 'time', 'device', 2);

INSERT INTO error_test VALUES ('Mon Mar 20 09:18:20.1 2017', 21.3, 'dev1');
\set ON_ERROR_STOP 0
-- generate insert error 
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:22.3 2017', 21.1, NULL);
\set ON_ERROR_STOP 1
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:25.7 2017', 22.4, 'dev2');
SELECT * FROM error_test;

--test character(9) partition keys since there were issues with padding causing partitioning errors
CREATE TABLE tick_character (
    symbol      character(9) NOT NULL,                                                                                                      mid       REAL NOT NULL,                                                                                                                spread      REAL NOT NULL,
    time        TIMESTAMPTZ       NOT NULL
);

SELECT create_hypertable ('tick_character', 'time', 'symbol', 2);
INSERT INTO tick_character ( symbol, mid, spread, time ) VALUES ( 'GBPJPY', 142.639000, 5.80, 'Mon Mar 20 09:18:22.3 2017');
SELECT * FROM tick_character;


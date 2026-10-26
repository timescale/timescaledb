-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test CHECK constraints on chunks with PG18+ features

-- CHECK constraint referencing a virtual generated column
CREATE TABLE ht_virtual (
    time timestamptz NOT NULL,
    reading int,
    doubled int GENERATED ALWAYS AS (reading * 2) VIRTUAL CHECK (doubled > 0)
) WITH (tsdb.hypertable, tsdb.partition_column = 'time');

INSERT INTO ht_virtual (time, reading) VALUES ('2025-04-11', 3);
\set ON_ERROR_STOP 0
INSERT INTO ht_virtual (time, reading) VALUES ('2025-04-11', -3);
\set ON_ERROR_STOP 1
SELECT * FROM ht_virtual ORDER BY time;

-- NOT ENFORCED CHECK constraint must not be checked
CREATE TABLE ht_not_enforced (
    time timestamptz NOT NULL,
    value int CHECK (value > 0) NOT ENFORCED
) WITH (tsdb.hypertable, tsdb.partition_column = 'time');

INSERT INTO ht_not_enforced VALUES ('2025-04-11', -1);
SELECT * FROM ht_not_enforced ORDER BY time;

DROP TABLE ht_virtual;
DROP TABLE ht_not_enforced;

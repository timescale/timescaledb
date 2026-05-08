-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test a case of hypertable expansion in a DML query where it is not a target
-- relation.

CREATE TABLE ht_target (time timestamptz NOT NULL, val int);
SELECT create_hypertable('ht_target', 'time');
INSERT INTO ht_target VALUES ('2024-03-15', 1);

CREATE TABLE ht_empty (time timestamptz NOT NULL, val int);
SELECT create_hypertable('ht_empty', 'time');

EXPLAIN (costs off)
UPDATE ht_target SET val = ht_empty.val
FROM ht_empty
WHERE ht_target.time = ht_empty.time;


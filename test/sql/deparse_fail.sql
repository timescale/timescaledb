-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ON_ERROR_STOP 0

CREATE TEMP TABLE fail_table1(x INT);

SELECT _timescaledb_internal.get_tabledef('fail_table1');

CREATE INDEX my_fail_table1_idx ON fail_table1 USING BTREE(x);

SELECT _timescaledb_internal.get_tabledef('my_fail_table1_idx');

SELECT _timescaledb_internal.get_tabledef('non_existing');

CREATE TABLE row_sec(i INT);
ALTER TABLE row_sec ENABLE ROW LEVEL SECURITY;
SELECT _timescaledb_internal.get_tabledef('row_sec');

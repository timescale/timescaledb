-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE gencol_tab (
    a INT NOT NULL,
    b INT,
    c INT GENERATED ALWAYS AS (a + b) STORED
);
SELECT table_name FROM create_hypertable('gencol_tab', 'a', chunk_time_interval=>10);

INSERT INTO gencol_tab(a, b) VALUES(1, 2);
INSERT INTO gencol_tab(a, b) VALUES(2, 3);

-- Ensure generated column cannot be updated
\set ON_ERROR_STOP 0
INSERT INTO gencol_tab VALUES(3, 5, 8);
\set ON_ERROR_STOP 1

SELECT * FROM gencol_tab ORDER BY a;

DROP TABLE gencol_tab;

-- Ensure that generated column cannot be used for partitioning

-- Generated as expression
CREATE TABLE gencol_test (
    a INT NOT NULL,
    b INT GENERATED ALWAYS AS (a + 123) STORED
);
\set ON_ERROR_STOP 0
SELECT table_name FROM create_hypertable('gencol_test', 'a', 'b', 2, chunk_time_interval=>10);
\set ON_ERROR_STOP 1

-- check if default generated expression can be dropped (works on >= PG13)
SELECT table_name FROM create_hypertable('gencol_test', 'a',  chunk_time_interval=>10);
SELECT attname, atthasdef, attidentity, attgenerated, attnotnull
FROM pg_attribute where attname = 'b' and attrelid = 'gencol_test'::regclass;
\set ON_ERROR_STOP 0
ALTER TABLE gencol_test ALTER COLUMN b DROP EXPRESSION;
\set ON_ERROR_STOP 1
SELECT attname, atthasdef, attidentity, attgenerated, attnotnull
FROM pg_attribute where attname = 'b' and attrelid = 'gencol_test'::regclass;

DROP TABLE gencol_test;



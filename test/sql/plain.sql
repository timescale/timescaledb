-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Tests for plain PostgreSQL commands to ensure that they work while
-- the TimescaleDB extension is loaded. This is a mix of statements
-- added mostly as regression checks when bugs are discovered and
-- fixed.

CREATE TABLE regular_table(time timestamp, temp float8, tag text, color integer);

-- Renaming indexes should work
CREATE INDEX time_color_idx ON regular_table(time, color);
ALTER INDEX time_color_idx RENAME TO time_color_idx2;
ALTER TABLE regular_table ALTER COLUMN color TYPE bigint;

SELECT * FROM test.show_columns('regular_table');
SELECT * FROM test.show_indexes('regular_table');

-- Renaming types should work
CREATE TYPE rainbow AS ENUM ('red', 'orange', 'yellow', 'green', 'blue', 'purple');
ALTER TYPE rainbow RENAME TO colors;

\dT+

REINDEX TABLE regular_table;
\c :TEST_DBNAME :ROLE_SUPERUSER
REINDEX SCHEMA public;

-- Not only simple statements should work
CREATE TABLE a (aa TEXT);
CREATE TABLE z (b TEXT, PRIMARY KEY(aa, b)) inherits (a);

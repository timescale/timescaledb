-- DROP a table's column before making it a hypertable
CREATE TABLE alter_before(id serial, time timestamp, temp float, colorid integer, notes text, notes_2 text);
ALTER TABLE alter_before DROP COLUMN id;
ALTER TABLE alter_before ALTER COLUMN temp SET (n_distinct = 10);
ALTER TABLE alter_before ALTER COLUMN colorid SET (n_distinct = 11);
ALTER TABLE alter_before ALTER COLUMN colorid RESET (n_distinct);
ALTER TABLE alter_before ALTER COLUMN temp SET STATISTICS 100;
ALTER TABLE alter_before ALTER COLUMN notes SET STORAGE EXTERNAL;

SELECT create_hypertable('alter_before', 'time', chunk_time_interval => 2628000000000);

INSERT INTO alter_before VALUES ('2017-03-22T09:18:22', 23.5, 1);

SELECT * FROM alter_before;

-- Show that deleted column is marked as dropped and that attnums are
-- now different for the root table and the chunk
SELECT c.relname, a.attname, a.attnum, a.attoptions, a.attstattarget, a.attstorage FROM pg_attribute a, pg_class c
WHERE a.attrelid = c.oid
AND (c.relname LIKE '_hyper_1%_chunk' OR c.relname = 'alter_before')
AND a.attnum > 0
ORDER BY c.relname, a.attnum;

-- DROP a table's column after making it a hypertable and having data
CREATE TABLE alter_after(id serial, time timestamp, temp float, colorid integer, notes text, notes_2 text);
SELECT create_hypertable('alter_after', 'time', chunk_time_interval => 2628000000000);

-- Create first chunk
INSERT INTO alter_after (time, temp, colorid) VALUES ('2017-03-22T09:18:22', 23.5, 1);

ALTER TABLE alter_after DROP COLUMN id;
ALTER TABLE alter_after ALTER COLUMN temp SET (n_distinct = 10);
ALTER TABLE alter_after ALTER COLUMN colorid SET (n_distinct = 11);
ALTER TABLE alter_after ALTER COLUMN colorid RESET (n_distinct);
ALTER TABLE alter_after ALTER COLUMN colorid SET STATISTICS 101;
ALTER TABLE alter_after ALTER COLUMN notes_2 SET STORAGE EXTERNAL;

-- Creating new chunks after dropping a column should work just fine
INSERT INTO alter_after VALUES ('2017-03-22T09:18:23', 21.5, 1),
                               ('2017-05-22T09:18:22', 36.2, 2),
                               ('2017-05-22T09:18:23', 15.2, 2);

-- Make sure tuple conversion also works with COPY
\COPY alter_after FROM 'data/alter.tsv' NULL AS '';

-- Data should look OK
SELECT * FROM alter_after;

-- Show that attnums are different for chunks created after DROP
-- column
SELECT c.relname, a.attname, a.attnum FROM pg_attribute a, pg_class c
WHERE a.attrelid = c.oid
AND (c.relname LIKE '_hyper_2%_chunk' OR c.relname = 'alter_after')
AND a.attnum > 0
ORDER BY c.relname, a.attnum;

-- Add an ID column again
ALTER TABLE alter_after ADD COLUMN id serial;

INSERT INTO alter_after (time, temp, colorid) VALUES ('2017-08-22T09:19:14', 12.5, 3);

--test thing that we are allowed to do on chunks
ALTER TABLE  _timescaledb_internal._hyper_2_3_chunk ALTER COLUMN temp RESET (n_distinct);
ALTER TABLE  _timescaledb_internal._hyper_2_4_chunk ALTER COLUMN temp SET (n_distinct = 20);
ALTER TABLE  _timescaledb_internal._hyper_2_4_chunk ALTER COLUMN temp SET STATISTICS 201;
ALTER TABLE  _timescaledb_internal._hyper_2_4_chunk ALTER COLUMN notes SET STORAGE EXTERNAL;

SELECT c.relname, a.attname, a.attnum, a.attoptions, a.attstattarget, a.attstorage FROM pg_attribute a, pg_class c
WHERE a.attrelid = c.oid
AND (c.relname LIKE '_hyper_2%_chunk' OR c.relname = 'alter_after')
AND a.attnum > 0
ORDER BY c.relname, a.attnum;

SELECT * FROM alter_after;

-- Need superuser to ALTER chunks in _timescaledb_internal schema
\c single :ROLE_SUPERUSER
SELECT * FROM _timescaledb_catalog.chunk WHERE id = 2;

-- Rename chunk
ALTER TABLE _timescaledb_internal._hyper_2_2_chunk RENAME TO new_chunk_name;
SELECT * FROM _timescaledb_catalog.chunk WHERE id = 2;

-- Set schema
ALTER TABLE _timescaledb_internal.new_chunk_name SET SCHEMA public;
SELECT * FROM _timescaledb_catalog.chunk WHERE id = 2;

-- Test that we cannot rename chunk columns
\set ON_ERROR_STOP 0
ALTER TABLE public.new_chunk_name RENAME COLUMN time TO newtime;
\set ON_ERROR_STOP 1

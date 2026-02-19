-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-------------------------------------------------------------------
-- Config tests
-------------------------------------------------------------------

CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";
CREATE VIEW compressedcols AS select relname,attname,c.oid as reloid,attnum from pg_attribute a, pg_class c where c.oid=a.attrelid and relname like '%compress_hyper_%chunk' order by c.oid asc, a.attnum asc;

CREATE TABLE t(a int, b int, c int, d int, e int, f int, g int, h int, i int);
SELECT create_hypertable('t', 'a');


-- Should succeed (8 columns max)
-- TODO: remove later, once composite blooms fully rolled out
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("a","b","c","d","e","f","g","h")');
\set ON_ERROR_STOP 1

select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings
where relid = 't'::regclass and index is not null order by 1,2;

-- Should fail (9 columns)
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("a","b","c","d","e","f","g","h","i")');
\set ON_ERROR_STOP 1

-- The ordering of bloom columns in the composite bloom index should be determined by the order of the columns in the CREATE TABLE statement
-- TODO: remove later, once composite blooms fully rolled out
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("h","g","f","e","d","c","b")');
\set ON_ERROR_STOP 1
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings
where relid = 't'::regclass and index is not null order by 1,2;

-- Creating two composite bloom indexes with the same columns in different orders should fail
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'b', timescaledb.compress_index = 'bloom("h","g","f"),bloom("f","g","h")');
\set ON_ERROR_STOP 1

-- Creating a composite bloom index with the same columns should fail
\set ON_ERROR_STOP 0
ALTER TABLE t SET (timescaledb.compress_index = 'bloom("a","b","a")');
ALTER TABLE t SET (timescaledb.compress_index = 'bloom("g","g")');
\set ON_ERROR_STOP 1

DROP TABLE t CASCADE;

-- Creating a composite bloom index based on a primary key, and create the same manually again
CREATE TABLE u(a int, b int, c int, d int, PRIMARY KEY (a, b, c));
SELECT create_hypertable('u', 'a');
ALTER TABLE u SET (timescaledb.compress, timescaledb.compress_orderby = 'b');
select index from settings where relid = 'u'::regclass and index is not null order by 1;
INSERT INTO u VALUES (1, 2, 3, 4), (5, 6, 7, 8);

select count(compress_chunk(x)) from show_chunks('u') x;
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

-- Check the auto generated compressed columns
select relname,attname from compressedcols order by 1,2;

-- TODO: remove later, once composite blooms fully rolled out
\set ON_ERROR_STOP 0
ALTER TABLE u SET (timescaledb.compress_index = 'bloom("a","b","c")');
\set ON_ERROR_STOP 1
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

-- Also in a different order
-- TODO: remove later, once composite blooms fully rolled out
\set ON_ERROR_STOP 0
ALTER TABLE u SET (timescaledb.compress_index = 'bloom("c","b","a")');
\set ON_ERROR_STOP 1
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

DROP TABLE u CASCADE;

-------------------------------------------------------------------
-- Long column names tests
-------------------------------------------------------------------

CREATE TABLE v(a_01234567890123456789 int, b_01234567890123456789 int, c_01234567890123456789 int, d_01234567890123456789 int, e_01234567890123456789 int, f_01234567890123456789 int, g_01234567890123456789 int, h_01234567890123456789 int, i_01234567890123456789 int, primary key (a_01234567890123456789, b_01234567890123456789, c_01234567890123456789, d_01234567890123456789, e_01234567890123456789));
SELECT create_hypertable('v', 'a_01234567890123456789');

ALTER TABLE v SET (timescaledb.compress, timescaledb.compress_orderby = 'b_01234567890123456789');
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;

INSERT INTO v VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9);
select count(compress_chunk(x)) from show_chunks('v') x;

-- Check the auto generated composite bloom index configuration and column names
select relid,compress_relid,segmentby,orderby,orderby_desc,orderby_nullsfirst,index from settings where index is not null order by 1,2;
select relname,attname from compressedcols order by 1,2;

-- Make sure the duplicate column detection works for long column names
\set ON_ERROR_STOP 0
ALTER TABLE v SET (timescaledb.compress_index = 'bloom("a_01234567890123456789","b_01234567890123456789","c_01234567890123456789","a_01234567890123456789")');
\set ON_ERROR_STOP 1

DROP TABLE v CASCADE;

-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--install necessary functions for tests
\c :TEST_DBNAME :ROLE_SUPERUSER
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Create some data with NULLs and bools
CREATE TABLE d (ts int, b bool);
INSERT INTO d SELECT g AS ts, NULL AS b FROM generate_series(1, 5000) g;

-- set b to true for even ts values and set some values to NULL
UPDATE d SET b = (ts % 2 = 0);
UPDATE d SET b = NULL WHERE (ts % 10 = 0);

-- add some bools that can be RLE compressed
INSERT INTO d SELECT g AS ts, true AS b FROM generate_series(5001, 20000) g;

-- add a few bool columns
CREATE TABLE t (ts int, b1 bool, b2 bool, b3 bool);
SELECT create_hypertable('t', 'ts', chunk_time_interval => 5000);

-- explicitly disable bool compression so the test
-- doesn't depend on the default setting
SET timescaledb.enable_bool_compression = off;
INSERT INTO t
SELECT
	d.ts,
	d.b AS b1, d.b AS b2, d.b AS b3
FROM d ORDER BY ts;
SELECT max(ts) FROM t;

ALTER TABLE t SET (timescaledb.compress, timescaledb.compress_orderby = 'ts');
SELECT compress_chunk(show_chunks('t'));

CREATE TABLE chunks_done AS SELECT show_chunks('t') AS chunk_name;
SELECT * FROM chunks_done;

SELECT
	chunk_schema, chunk_name, compression_status,
	after_compression_total_bytes
	-- the before compression size differs on platforms, so I dont't
	-- display it here, and consequently the compression ratio as well
FROM
	chunk_compression_stats('t');

-- enable bool compression and add more data, so the two compression
-- methods will co-exist and we can test both, plus compare the
-- compression ratio
--
SET timescaledb.enable_bool_compression = on;
INSERT INTO t
SELECT
	(SELECT max(ts) FROM t)+d.ts,
	d.b AS b1, d.b AS b2, d.b AS b3
FROM d ORDER BY ts;
SELECT max(ts) FROM t;

SELECT
	compress_chunk(c)
FROM
	show_chunks('t') c
WHERE
	c NOT IN (SELECT chunk_name FROM chunks_done);

SELECT
	chunk_schema, chunk_name, compression_status,
	after_compression_total_bytes
	-- the before compression size differs on platforms, so I dont't
	-- display it here, and consequently the compression ratio as well
	--
	-- the after compression size should be smaller than it was before
	-- the bool compression was enabled
	--
FROM
	chunk_compression_stats('t')
WHERE
	format('%I.%I', chunk_schema, chunk_name)::regclass NOT IN (SELECT chunk_name FROM chunks_done);


-- check the compression algorithm for the compressed chunks
CREATE TABLE compressed_chunks AS
SELECT
	format('%I.%I', comp.schema_name, comp.table_name)::regclass as compressed_chunk,
	ccs.compressed_heap_size,
	ccs.compressed_toast_size,
	ccs.compressed_index_size,
	ccs.numrows_pre_compression,
	ccs.numrows_post_compression
FROM
	show_chunks('t') c
	INNER JOIN _timescaledb_catalog.chunk cat
		ON (c = format('%I.%I', cat.schema_name, cat.table_name)::regclass)
	INNER JOIN _timescaledb_catalog.chunk comp
		ON (cat.compressed_chunk_id = comp.id)
	INNER JOIN _timescaledb_catalog.compression_chunk_size ccs
		ON (comp.id = ccs.compressed_chunk_id);

CREATE TABLE compression_info (compressed_chunk regclass, result text, compressed_size int, num_rows int);

DO $$
DECLARE
	table_ref regclass;
BEGIN
	FOR table_ref IN
		SELECT compressed_chunk as table_ref FROM compressed_chunks
	LOOP
		EXECUTE format(
			'INSERT INTO compression_info (
				SELECT
					%L::regclass as compressed_chunk,
					(_timescaledb_functions.compressed_data_info(b1))::text as result,
					sum(pg_column_size(b1)::int) as compressed_size,
					count(*) as num_rows
				FROM %s
				GROUP BY 1,2)',
			table_ref, table_ref
		);
	END LOOP;
END;
$$;

SELECT
	ci.*,
	ccs.compressed_toast_size,
	ccs.numrows_pre_compression,
	ccs.numrows_post_compression
FROM
	compression_info ci
	INNER JOIN compressed_chunks ccs
		ON (ci.compressed_chunk = ccs.compressed_chunk)
ORDER BY
	1,2,3;

DROP TABLE t;
DROP TABLE d;
DROP TABLE chunks_done;
DROP TABLE compression_info;

-- reset the compression setting
SET timescaledb.enable_bool_compression = default;

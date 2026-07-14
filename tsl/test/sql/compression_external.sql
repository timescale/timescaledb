-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.
--
-- Test the EXTERNAL compression algorithm. A type provides
-- <typename>_compress(<type>[]) RETURNS bytea and
-- <typename>_decompress(bytea) RETURNS <type>[] in its own schema.
\c :TEST_DBNAME :ROLE_SUPERUSER

-- A toy int wrapper type whose "compression" just mirrors the bits of each value.
CREATE TYPE mirrorint;
CREATE FUNCTION mirrorint_in(cstring) RETURNS mirrorint AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION mirrorint_out(mirrorint) RETURNS cstring AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE mirrorint (
	INPUT = mirrorint_in,
	OUTPUT = mirrorint_out,
	INTERNALLENGTH = 4,
	PASSEDBYVALUE,
	ALIGNMENT = int4
);

CREATE CAST (mirrorint AS int4) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (int4 AS mirrorint) WITHOUT FUNCTION AS IMPLICIT;

CREATE FUNCTION mirror_bits(x int4) RETURNS int4
AS $$ SELECT reverse(x::bit(32)::text)::bit(32)::int4 $$
LANGUAGE sql IMMUTABLE STRICT;
CREATE FUNCTION mirrorint_compress(batch mirrorint[]) RETURNS bytea
AS $$
	SELECT coalesce(string_agg(int4send(mirror_bits(v::int4)), ''::bytea ORDER BY ord), ''::bytea)
	FROM unnest(batch) WITH ORDINALITY AS u(v, ord)
$$ LANGUAGE sql IMMUTABLE STRICT;

CREATE FUNCTION mirrorint_decompress(payload bytea) RETURNS mirrorint[]
AS $$
DECLARE
	result mirrorint[] := '{}';
BEGIN
	FOR i IN 0 .. octet_length(payload) / 4 - 1 LOOP
		result := result ||
			mirror_bits(('x' || encode(substring(payload FROM i * 4 + 1 FOR 4), 'hex'))::bit(32)::int4)::mirrorint;
	END LOOP;
	RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- the codec round-trips on its own, preserving order and extreme values
SELECT mirrorint_decompress(mirrorint_compress('{1, -1, 0, 2147483647, -2147483648}'::mirrorint[]));

-- a hypertable using the type; NULLs and > 1000 rows to span batches
CREATE TABLE toy (ts int NOT NULL, v mirrorint);
SELECT create_hypertable('toy', 'ts', chunk_time_interval => 10000);
INSERT INTO toy
	SELECT g, CASE WHEN g % 10 = 0 THEN NULL ELSE (g - 1250)::int4::mirrorint END
	FROM generate_series(1, 2500) g;
INSERT INTO toy VALUES (2501, 2147483647), (2502, -2147483648), (2503, 0);
CREATE TABLE toy_copy AS SELECT * FROM toy;

ALTER TABLE toy SET (timescaledb.compress, timescaledb.compress_orderby = 'ts');
SELECT compress_chunk(show_chunks('toy'));

-- the column was compressed with the EXTERNAL algorithm
SELECT format('%s', cs.compress_relid) AS compressed_chunk
FROM show_chunks('toy') c
INNER JOIN _timescaledb_catalog.chunk cat ON (c = cat.relid)
INNER JOIN _timescaledb_catalog.compression_settings cs ON (cs.relid = cat.relid) \gset

SELECT DISTINCT (_timescaledb_functions.compressed_data_info(v)).algorithm,
	(_timescaledb_functions.compressed_data_info(v)).has_nulls
FROM :compressed_chunk;

-- data round-trips losslessly through compression (both directions)
SELECT count(*) AS missing FROM (
	SELECT ts, v::int4 FROM toy_copy EXCEPT SELECT ts, v::int4 FROM toy) d;
SELECT count(*) AS extraneous FROM (
	SELECT ts, v::int4 FROM toy EXCEPT SELECT ts, v::int4 FROM toy_copy) d;

-- forward and reverse iteration over compressed data
SELECT ts, v FROM toy ORDER BY ts LIMIT 3;
SELECT ts, v FROM toy ORDER BY ts DESC LIMIT 3;

-- aggregates match the uncompressed copy
SELECT sum(v::int4), count(v), count(*) FROM toy;
SELECT sum(v::int4), count(v), count(*) FROM toy_copy;

-- DML on the compressed chunk
INSERT INTO toy VALUES (5000, 42);
SELECT ts, v FROM toy WHERE ts = 5000;

-- decompression restores the original rows
SELECT decompress_chunk(show_chunks('toy'));
SELECT count(*) AS mismatches FROM (
	SELECT ts, v::int4 FROM toy WHERE ts <= 2503
	EXCEPT SELECT ts, v::int4 FROM toy_copy) d;

-- reads of compressed data fail loudly if the type's codec goes missing
SELECT compress_chunk(show_chunks('toy'));
DROP FUNCTION mirrorint_decompress(bytea);
\set ON_ERROR_STOP 0
SELECT sum(v::int4) FROM toy;
\set ON_ERROR_STOP 1
CREATE FUNCTION mirrorint_decompress(payload bytea) RETURNS mirrorint[]
AS $$
DECLARE
	result mirrorint[] := '{}';
BEGIN
	FOR i IN 0 .. octet_length(payload) / 4 - 1 LOOP
		result := result ||
			mirror_bits(('x' || encode(substring(payload FROM i * 4 + 1 FOR 4), 'hex'))::bit(32)::int4)::mirrorint;
	END LOOP;
	RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
SELECT sum(v::int4) FROM toy;

-- a wrapper type without codec functions still falls back to ARRAY
CREATE TYPE plainint;
CREATE FUNCTION plainint_in(cstring) RETURNS plainint AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION plainint_out(plainint) RETURNS cstring AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE plainint (
	INPUT = plainint_in,
	OUTPUT = plainint_out,
	INTERNALLENGTH = 4,
	PASSEDBYVALUE,
	ALIGNMENT = int4
);
CREATE TABLE toy_plain (ts int NOT NULL, v plainint);
SELECT create_hypertable('toy_plain', 'ts', chunk_time_interval => 10000);
INSERT INTO toy_plain SELECT g, '7'::plainint FROM generate_series(1, 100) g;
ALTER TABLE toy_plain SET (timescaledb.compress, timescaledb.compress_orderby = 'ts');
SELECT compress_chunk(show_chunks('toy_plain'));

SELECT format('%s', cs.compress_relid) AS plain_compressed_chunk
FROM show_chunks('toy_plain') c
INNER JOIN _timescaledb_catalog.chunk cat ON (c = cat.relid)
INNER JOIN _timescaledb_catalog.compression_settings cs ON (cs.relid = cat.relid) \gset

SELECT DISTINCT (_timescaledb_functions.compressed_data_info(v)).algorithm
FROM :plain_compressed_chunk;

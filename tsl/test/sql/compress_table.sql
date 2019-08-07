-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_compress_table(in_table REGCLASS, out_table REGCLASS, compression_info _timescaledb_catalog.hypertable_compression[])
    RETURNS VOID
    AS :TSL_MODULE_PATHNAME LANGUAGE C STRICT VOLATILE;
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- column name, algorithm, idx, asc, nulls_first
CREATE FUNCTION ord(TEXT, INT, INT, BOOL = true, BOOL = false)
    RETURNS _timescaledb_catalog.hypertable_compression
    AS $$
        SELECT (1, $1, $2::SMALLINT, -1, $3::SMALLINT, $4, $5)::_timescaledb_catalog.hypertable_compression
    $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- column name, idx, asc, nulls_first
CREATE FUNCTION seg(TEXT, INT, BOOL = true, BOOL = false)
    RETURNS _timescaledb_catalog.hypertable_compression
    AS $$
        SELECT (1, $1, 0, $2::SMALLINT, -1, $3, $4)::_timescaledb_catalog.hypertable_compression
    $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- column name, algorithm
CREATE FUNCTION com(TEXT, INT)
    RETURNS _timescaledb_catalog.hypertable_compression
    AS $$
        SELECT (1, $1, $2::SMALLINT, -1, -1, true, false)::_timescaledb_catalog.hypertable_compression
    $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

SELECT * FROM ord('time', 4, 0);

CREATE TABLE uncompressed(
    time   INT,
    device INT,
    data   INT,
    floats FLOAT(26),
    nulls  TEXT,
    texts  TEXT);

CREATE TABLE compressed(
    time   _timescaledb_internal.compressed_data,
    device INT,
    data   _timescaledb_internal.compressed_data,
    floats _timescaledb_internal.compressed_data,
    nulls  _timescaledb_internal.compressed_data,
    texts  _timescaledb_internal.compressed_data);

\set DATA_IN uncompressed
\set DATA_OUT uncompressed

-- 	_INVALID_COMPRESSION_ALGORITHM = 0,
-- 	COMPRESSION_ALGORITHM_ARRAY = 1,
-- 	COMPRESSION_ALGORITHM_DICTIONARY = 2,
-- 	COMPRESSION_ALGORITHM_GORILLA = 3,
-- 	COMPRESSION_ALGORITHM_DELTADELTA = 4,

SELECT ARRAY[ord('time', 4, 0), seg('device', 0), com('data', 4), com('floats', 3), com('nulls', 1), com('texts', 2)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

-- TODO NULL decompression doesn't quite work
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(data::_timescaledb_internal.compressed_data, NULL::INT) d, _timescaledb_internal.decompress_forward(floats::_timescaledb_internal.compressed_data, NULL::FLOAT(26)) f, NULL, _timescaledb_internal.decompress_forward(texts::_timescaledb_internal.compressed_data, NULL::TEXT) e

INSERT INTO uncompressed
    SELECT generate_series( 1, 5), d, d % 3, d / 3.0, NULL, d
    FROM generate_series(1, 5) d;

INSERT INTO uncompressed
    SELECT generate_series(6,10), d, d % 2, d / 2.0, NULL, d
    FROM generate_series(1, 4) d;

INSERT INTO uncompressed
    SELECT generate_series(11,15), d, d    , d      , NULL, d
    FROM generate_series(1, 5) d;

INSERT INTO uncompressed
    SELECT generate_series(16,20), d, d % 3, d / 3.0, NULL, d
    FROM generate_series(1, 5) d;

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test gorilla on ints
SELECT ARRAY[ord('time', 4, 0), seg('device', 0), com('data', 3), com('floats', 3), com('nulls', 1), com('texts', 2)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test Dictionary on everything
SELECT ARRAY[ord('time', 2, 0), seg('device', 0), com('data', 2), com('floats', 2), com('nulls', 2), com('texts', 2)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test Array on everything
SELECT ARRAY[ord('time', 1, 0), seg('device', 0), com('data', 1), com('floats', 1), com('nulls', 1), com('texts', 1)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

--test reordering compression info
SELECT ARRAY[com('floats', 3), com('data', 4), seg('device', 0), ord('time', 4, 0), com('nulls', 1), com('texts', 2)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset
\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test dropping a column
ALTER TABLE uncompressed DROP COLUMN data;
ALTER TABLE uncompressed DROP COLUMN nulls;
ALTER TABLE compressed DROP COLUMN data;
ALTER TABLE compressed DROP COLUMN nulls;

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(floats::_timescaledb_internal.compressed_data, NULL::FLOAT(26)) f, _timescaledb_internal.decompress_forward(texts::_timescaledb_internal.compressed_data, NULL::TEXT) e

SELECT ARRAY[ord('time', 4, 0), seg('device', 0), com('floats', 3), com('texts', 2)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test adding a column
ALTER TABLE uncompressed ADD COLUMN dat2 INT DEFAULT 1;
ALTER TABLE uncompressed ADD COLUMN ord INT DEFAULT 2;
ALTER TABLE compressed ADD COLUMN dat2 _timescaledb_internal.compressed_data;
ALTER TABLE compressed ADD COLUMN ord _timescaledb_internal.compressed_data;

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(floats::_timescaledb_internal.compressed_data, NULL::FLOAT(26)) f, _timescaledb_internal.decompress_forward(texts::_timescaledb_internal.compressed_data, NULL::TEXT) e, _timescaledb_internal.decompress_forward(dat2::_timescaledb_internal.compressed_data, NULL::INT) d2, _timescaledb_internal.decompress_forward(ord::_timescaledb_internal.compressed_data, NULL::INT) o

SELECT ARRAY[ord('time', 4, 0), seg('device', 0), com('floats', 3), com('texts', 2), ord('ord', 4, 1), com('dat2', 4)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test skipping columns
CREATE TABLE missing_columns AS SELECT time, device, dat2 FROM uncompressed;
\set DATA_OUT missing_columns

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(dat2::_timescaledb_internal.compressed_data, NULL::INT) d2

SELECT ARRAY[ord('time', 4, 0), seg('device', 0), com('dat2', 4)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;


\set ON_ERROR_STOP 0

-- test compressing a non-existent column
SELECT ARRAY[ord('time', 4, 0), seg('device', 0), com('floats', 3), com('texts', 2), ord('ord', 4, 1), com('dat2', 4), com('fictional', 4)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

SELECT ts_compress_table(:'DATA_IN'::REGCLASS, 'compressed'::REGCLASS,:'COMPRESSION_INFO'::_timescaledb_catalog.hypertable_compression[]);
TRUNCATE compressed;

\set ON_ERROR_STOP 1

TRUNCATE uncompressed;

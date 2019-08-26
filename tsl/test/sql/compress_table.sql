-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_compress_table(in_table REGCLASS, out_table REGCLASS, compression_info _timescaledb_catalog.hypertable_compression[])
    RETURNS VOID
    AS :TSL_MODULE_PATHNAME LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION ts_decompress_table(in_table REGCLASS, out_table REGCLASS)
    RETURNS VOID
    AS :TSL_MODULE_PATHNAME LANGUAGE C STRICT VOLATILE;
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- column name, algorithm, idx, asc, nulls_first
--no sgement_byindex (use 0 to indicate that)
CREATE FUNCTION ord(TEXT, INT, INT, BOOL = true, BOOL = false)
    RETURNS _timescaledb_catalog.hypertable_compression
    AS $$
        SELECT (1, $1, $2::SMALLINT, 0, $3::SMALLINT+1, $4, $5)::_timescaledb_catalog.hypertable_compression
    $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- column name, idx, asc, nulls_first
-- no orderby_index. use 0 to indicate that.
CREATE FUNCTION seg(TEXT, INT, BOOL = true, BOOL = false)
    RETURNS _timescaledb_catalog.hypertable_compression
    AS $$
        SELECT (1, $1, 0, $2::SMALLINT+1, 0, $3, $4)::_timescaledb_catalog.hypertable_compression
    $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- column name, algorithm
--no orderby or segment by index (use 0 to indicate that)
CREATE FUNCTION com(TEXT, INT)
    RETURNS _timescaledb_catalog.hypertable_compression
    AS $$
        SELECT (1, $1, $2::SMALLINT, 0, 0, true, false)::_timescaledb_catalog.hypertable_compression
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
    _ts_meta_count int,
     _ts_meta_min_max_1 _timescaledb_internal.segment_meta_min_max,
     _ts_meta_min_max_2 _timescaledb_internal.segment_meta_min_max,
    time   _timescaledb_internal.compressed_data,
    device INT,
    data   _timescaledb_internal.compressed_data,
    floats _timescaledb_internal.compressed_data,
    nulls  _timescaledb_internal.compressed_data,
    texts  _timescaledb_internal.compressed_data);

\set DATA_IN uncompressed
\set DATA_OUT uncompressed

-- compression algorithms
\set array      1
\set dictionary 2
\set gorilla    3
\set deltadelta 4

SELECT ARRAY[ord('time', :deltadelta, 0), seg('device', 0), com('data', :deltadelta), com('floats', :gorilla), com('nulls', :array), com('texts', :dictionary)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

-- TODO NULL decompression doesn't quite work
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(data::_timescaledb_internal.compressed_data, NULL::INT) d, _timescaledb_internal.decompress_forward(floats::_timescaledb_internal.compressed_data, NULL::FLOAT(26)) f,  (NULL::text) n, _timescaledb_internal.decompress_forward(texts::_timescaledb_internal.compressed_data, NULL::TEXT) e

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
SELECT ARRAY[ord('time', :deltadelta, 0), seg('device', 0), com('data', :gorilla), com('floats', :gorilla), com('nulls', :array), com('texts', :dictionary)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test Dictionary on everything
SELECT ARRAY[ord('time', :dictionary, 0), seg('device', 0), com('data', :dictionary), com('floats', :dictionary), com('nulls', :dictionary), com('texts', :dictionary)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test Array on everything
SELECT ARRAY[ord('time', :array, 0), seg('device', 0), com('data', :array), com('floats', :array), com('nulls', :array), com('texts', :array)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

--test reordering compression info
SELECT ARRAY[com('floats', :gorilla), com('data', :deltadelta), seg('device', 0), ord('time', :deltadelta, 0), com('nulls', :array), com('texts', :dictionary)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset
\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test dropping a column
ALTER TABLE uncompressed DROP COLUMN data;
ALTER TABLE uncompressed DROP COLUMN nulls;
ALTER TABLE compressed DROP COLUMN data;
ALTER TABLE compressed DROP COLUMN nulls;

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(floats::_timescaledb_internal.compressed_data, NULL::FLOAT(26)) f, _timescaledb_internal.decompress_forward(texts::_timescaledb_internal.compressed_data, NULL::TEXT) e

SELECT ARRAY[ord('time', :deltadelta, 0), seg('device', 0), com('floats', :gorilla), com('texts', :dictionary)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test adding a column
ALTER TABLE uncompressed ADD COLUMN dat2 INT DEFAULT 1;
ALTER TABLE uncompressed ADD COLUMN ord INT DEFAULT 2;
ALTER TABLE compressed ADD COLUMN dat2 _timescaledb_internal.compressed_data;
ALTER TABLE compressed ADD COLUMN ord _timescaledb_internal.compressed_data;

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(floats::_timescaledb_internal.compressed_data, NULL::FLOAT(26)) f, _timescaledb_internal.decompress_forward(texts::_timescaledb_internal.compressed_data, NULL::TEXT) e, _timescaledb_internal.decompress_forward(dat2::_timescaledb_internal.compressed_data, NULL::INT) d2, _timescaledb_internal.decompress_forward(ord::_timescaledb_internal.compressed_data, NULL::INT) o

SELECT ARRAY[ord('time', :deltadelta, 0), seg('device', 0), com('floats', :gorilla), com('texts', :dictionary), ord('ord', :deltadelta, 1), com('dat2', :deltadelta)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;

-- test skipping columns
CREATE TABLE missing_columns AS SELECT time, device, dat2 FROM uncompressed;
\set DATA_OUT missing_columns

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::INT) t, device, _timescaledb_internal.decompress_forward(dat2::_timescaledb_internal.compressed_data, NULL::INT) d2

SELECT ARRAY[ord('time', :deltadelta, 0), seg('device', 0), com('dat2', :deltadelta)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\ir include/compress_table_test.sql
TRUNCATE compressed;


\set ON_ERROR_STOP 0

-- test compressing a non-existent column
SELECT ARRAY[ord('time', :deltadelta, 0), seg('device', 0), com('floats', :gorilla), com('texts', :dictionary), ord('ord', :deltadelta, 1), com('dat2', :deltadelta), com('fictional', :deltadelta)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

SELECT ts_compress_table(:'DATA_IN'::REGCLASS, 'compressed'::REGCLASS,:'COMPRESSION_INFO'::_timescaledb_catalog.hypertable_compression[]);
TRUNCATE compressed;

\set ON_ERROR_STOP 1
TRUNCATE uncompressed;


DROP TABLE compressed;
DROP TABLE uncompressed;

-- test other types
CREATE TABLE uncompressed(
    b BOOL,
    device SMALLINT,
    time FLOAT);

CREATE TABLE compressed(
    _ts_meta_count int,
     _ts_meta_min_max_1 _timescaledb_internal.segment_meta_min_max,
    b _timescaledb_internal.compressed_data,
    device _timescaledb_internal.compressed_data,
    time _timescaledb_internal.compressed_data);

\set DATA_IN uncompressed
\set DATA_OUT uncompressed

INSERT INTO uncompressed SELECT (i % 3)::BOOL, i, i / 3 FROM generate_series(1, 20) i;

SELECT ARRAY[ord('device', :deltadelta, 0), com('b', :deltadelta), com('time', :gorilla)]::_timescaledb_catalog.hypertable_compression[] AS "COMPRESSION_INFO" \gset

\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(b::_timescaledb_internal.compressed_data, NULL::BOOL) b, _timescaledb_internal.decompress_forward(device::_timescaledb_internal.compressed_data, NULL::SMALLINT) device, _timescaledb_internal.decompress_forward(time::_timescaledb_internal.compressed_data, NULL::FLOAT) t
\ir include/compress_table_test.sql
TRUNCATE compressed;
TRUNCATE uncompressed;

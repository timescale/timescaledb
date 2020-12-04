-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test1 ("Time" timestamptz, intcol integer, bntcol bigint, txtcol text);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 day');

INSERT INTO test1 
SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-05 1:00', '1 hour') t;

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = 'bntcol', timescaledb.compress_orderby = '"Time" DESC');

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
)
AS sub;

-- TEST: ALTER TABLE add column tests --
ALTER TABLE test1 ADD COLUMN new_coli integer;
ALTER TABLE test1 ADD COLUMN new_colv varchar(30);

SELECT * FROM _timescaledb_catalog.hypertable_compression 
ORDER BY attname;

SELECT count(*) from test1 where new_coli is not null;
SELECT count(*) from test1 where new_colv is null;

--decompress 1 chunk and query again 
SELECT COUNT(*) AS count_compressed
FROM
(
SELECT decompress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NOT NULL ORDER BY chunk.id
LIMIT 1
)
AS sub;

SELECT count(*) from test1 where new_coli is not null;
SELECT count(*) from test1 where new_colv is null;

--compress all chunks and query ---
--create new chunk and fill in data --
INSERT INTO test1 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text , 100, '101t' 
FROM generate_series('2018-03-08 1:00'::TIMESTAMPTZ, '2018-03-09 1:00', '1 hour') t;
SELECT count(*) from test1 where new_coli  = 100;
SELECT count(*) from test1 where new_colv  = '101t';

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
)
AS sub;
SELECT count(*) from test1 where new_coli  = 100;
SELECT count(*) from test1 where new_colv  = '101t';

CREATE INDEX new_index ON test1(new_colv);



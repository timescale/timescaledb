-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (costs off, timing off, summary off)'

CREATE OR REPLACE VIEW compressed_chunk_info_view AS
SELECT
   h.schema_name AS hypertable_schema,
   h.table_name AS hypertable_name,
   c.schema_name || '.' || c.table_name as chunk_name,
   c.status as chunk_status
FROM
   _timescaledb_catalog.hypertable h JOIN
  _timescaledb_catalog.chunk c ON h.id = c.hypertable_id
   LEFT JOIN _timescaledb_catalog.chunk comp
ON comp.id = c.compressed_chunk_id
;

CREATE TABLE sample_table (
       time TIMESTAMP WITH TIME ZONE NOT NULL,
       sensor_id INTEGER NOT NULL,
       cpu double precision null,
       temperature double precision null,
       name varchar(100) default 'this is a default string value'
);
CREATE INDEX sense_idx ON sample_table (sensor_id);

SELECT * FROM create_hypertable('sample_table', 'time',
       chunk_time_interval => INTERVAL '2 months');

\set start_date '2022-01-28 01:09:53.583252+05:30'

INSERT INTO sample_table
    SELECT
       	time + (INTERVAL '1 minute' * random()) AS time,
       		sensor_id,
       		random() AS cpu,
       		random()* 100 AS temperature
       	FROM
       		generate_series(:'start_date'::timestamptz - INTERVAL '1 months',
                            :'start_date'::timestamptz - INTERVAL '1 week',
                            INTERVAL '1 hour') AS g1(time),
       		generate_series(1, 8, 1 ) AS g2(sensor_id)
       	ORDER BY
       		time;

\set start_date '2023-03-17 17:51:11.322998+05:30'

-- insert into new chunks
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 12, 21.98, 33.123, 'new row1');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 12, 17.66, 13.875, 'new row1');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 13, 21.98, 33.123, 'new row2');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 9, 21.98, 33.123, 'new row2');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 14, 21.98, 33.123, 'new row2');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 15, 0.988, 33.123, 'new row3');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 16, 4.6554, 47, 'new row3');

-- Non-int, date, timestamp cannot be specified as a min/max ranges for now
-- We could expand to FLOATs, NUMERICs later
\set ON_ERROR_STOP 0
SELECT * FROM enable_column_stats('sample_table', 'name');
\set ON_ERROR_STOP 1

-- Specify tracking of min/max ranges for a column
SELECT * FROM enable_column_stats('sample_table', 'sensor_id');

-- The above should add an entry with MIN/MAX int64 entries for invalid chunk id
-- to indicate that ranges on this column should be calculated for chunks
SELECT * from _timescaledb_catalog.chunk_column_stats;

-- Skipping should work
SELECT * FROM enable_column_stats('sample_table', 'sensor_id', true);

-- A query using a WHERE clause on "sensor_id" column will scan all the chunks
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- For the purposes of min/max range tracking, a compressed chunk is considered as a
-- completed chunk.

-- enable compression
ALTER TABLE sample_table SET (
	timescaledb.compress,
    timescaledb.compress_orderby = 'time'
);

--
-- compress one chunk
SELECT show_chunks('sample_table') AS "CH_NAME" order by 1 limit 1 \gset
SELECT compress_chunk(:'CH_NAME');

SELECT id AS "CHUNK_ID" from _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' \gset

-- There should be an entry with min/max range computed for this chunk for this
-- "sensor_id" column.
SELECT * from _timescaledb_catalog.chunk_column_stats where chunk_id = :'CHUNK_ID';

-- check chunk compression status
SELECT chunk_status
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' AND chunk_name = :'CH_NAME';


:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- do update, this will change the status of the chunk
UPDATE sample_table SET name = 'updated row' WHERE cpu = 21.98 AND temperature = 33.123;

-- check chunk compression status
SELECT chunk_status
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' AND chunk_name = :'CH_NAME';

-- There should be an entry with "valid" set to false for this chunk
SELECT * from _timescaledb_catalog.chunk_column_stats WHERE chunk_id = :'CHUNK_ID';

-- A query using a WHERE clause on "sensor_id" column will go back to scanning all the chunks
-- along with an expensive DECOMPRESS on the first chunk
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- Remove the index to check the sequential min/max calculation code as well
DROP INDEX sense_idx;

-- recompress the partial chunk
SELECT compress_chunk(:'CH_NAME');

-- check chunk compression status
SELECT chunk_status
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' AND chunk_name = :'CH_NAME';

-- The chunk entry should become "valid" again
SELECT * from _timescaledb_catalog.chunk_column_stats WHERE chunk_id = :'CHUNK_ID';

-- A query using a WHERE clause on "sensor_id" column will scan the proper chunk
-- due to chunk exclusion using min/max ranges calculated above
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- Newly added chunks should also have MIN/MAX entry
\set start_date '2024-01-28 01:09:51.583252+05:30'
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 1, 9.6054, 78.999, 'new row4');
SELECT * from _timescaledb_catalog.chunk_column_stats;

-- use the disable_column_stats API to remove the min/max range entries
SELECT * from disable_column_stats('sample_table', 'sensor_id');
SELECT * from _timescaledb_catalog.chunk_column_stats;
SELECT * from disable_column_stats('sample_table', 'sensor_id', true);

\set ON_ERROR_STOP 0
SELECT * from disable_column_stats('sample_table', 'sensor_id');
-- should only work on columns that have been enabled for tracking
SELECT * from disable_column_stats('sample_table', 'time');
SELECT * from disable_column_stats('sample_table', 'cpu');
\set ON_ERROR_STOP 1

-- Check that a DROP COLUMN removes entries from catalogs as well
SELECT * FROM enable_column_stats('sample_table', 'sensor_id');
SELECT decompress_chunk(:'CH_NAME');
-- Entry should be removed for this chunk now
SELECT * from _timescaledb_catalog.chunk_column_stats;

-- disable compression to allow dropping of the column
ALTER TABLE sample_table SET (
	timescaledb.compress = FALSE
);
ALTER TABLE sample_table DROP COLUMN sensor_id;
SELECT * from _timescaledb_catalog.chunk_column_stats;

DROP TABLE sample_table;

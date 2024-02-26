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

-- Non-int, date, timestamp cannot be specified as a correlated constraint for now
-- We could expand to FLOATs, NUMERICs later
\set ON_ERROR_STOP 0
SELECT * FROM add_dimension('sample_table', by_correlation('name'));
\set ON_ERROR_STOP 1

-- Specify a correlated constraint
SELECT * FROM add_dimension('sample_table', by_correlation('sensor_id'));

-- The above should add a dimension_slice entry with MIN/MAX int64 entries and all
-- existing chunks will point to this to indicate that there is no chunk exclusion
-- yet if this correlated constraint column is used in WHERE clauses
SELECT id AS dimension_id FROM _timescaledb_catalog.dimension WHERE type = 'r' \gset
-- should show MIN_INT/MAX_INT entries
SELECT * FROM _timescaledb_catalog.dimension WHERE type = 'r' AND id = :dimension_id;
SELECT id AS slice_id FROM _timescaledb_catalog.dimension_slice WHERE dimension_id = :dimension_id \gset
SELECT * FROM _timescaledb_catalog.chunk_constraint WHERE dimension_slice_id = :slice_id ORDER BY chunk_id;

-- A query using a WHERE clause on "sensor_id" column will scan all the chunks
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- For the purposes of correlated constraints, a compressed chunk is considered as a
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

-- There should be an entry with min/max range computed for this chunk for this
-- "sensor_id" column
SELECT * FROM _timescaledb_catalog.dimension_slice WHERE dimension_id = :dimension_id;
SELECT * FROM _timescaledb_catalog.chunk_constraint WHERE constraint_name LIKE '_$CC_con%' ORDER BY chunk_id;

-- check chunk compression status
SELECT chunk_status
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' AND chunk_name = :'CH_NAME';

-- A query using a WHERE clause on "sensor_id" column will scan the proper chunk
-- due to chunk exclusion using correlated constraints ranges calculated above
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- do update, this will change the status of the chunk
UPDATE sample_table SET name = 'updated row' WHERE cpu = 21.98 AND temperature = 33.123;

-- check chunk compression status
SELECT chunk_status
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' AND chunk_name = :'CH_NAME';

-- The chunk_constraint should point to the MIN_INT/MAX_INT entry now
SELECT * FROM _timescaledb_catalog.chunk_constraint WHERE constraint_name LIKE '_$CC_con%' ORDER BY chunk_id;

-- A query using a WHERE clause on "sensor_id" column will go back to scanning all the chunks
-- along with an expensive DECOMPRESS on the first chunk
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- recompress the partial chunk
SELECT compress_chunk(:'CH_NAME');

-- check chunk compression status
SELECT chunk_status
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' AND chunk_name = :'CH_NAME';

-- There should be an entry with min/max range computed for this chunk
SELECT * FROM _timescaledb_catalog.dimension_slice WHERE dimension_id = :dimension_id;
SELECT * FROM _timescaledb_catalog.chunk_constraint WHERE constraint_name LIKE '_$CC_con%' ORDER BY chunk_id;

-- A query using a WHERE clause on "sensor_id" column will scan the proper chunk
-- due to chunk exclusion using correlated constraints ranges calculated above
:PREFIX SELECT * FROM sample_table WHERE sensor_id > 9;

-- Newly added chunks should also point to this MIN/MAX entry
\set start_date '2024-01-28 01:09:51.583252+05:30'
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 1, 9.6054, 78.999, 'new row4');
SELECT * FROM _timescaledb_catalog.chunk_constraint WHERE dimension_slice_id = :slice_id ORDER BY chunk_id;

-- use the remove_dimension API to remove the correlated dimension entries
SELECT dimension_id AS dim_id from remove_dimension('sample_table', 'sensor_id') \gset
SELECT * FROM _timescaledb_catalog.dimension_slice WHERE dimension_id = :dim_id;
SELECT * FROM _timescaledb_catalog.dimension WHERE id = :dim_id;
SELECT * FROM _timescaledb_catalog.dimension WHERE type = 'r';
SELECT * from remove_dimension('sample_table', 'sensor_id', true);
\set ON_ERROR_STOP 0
SELECT * from remove_dimension('sample_table', 'sensor_id');
-- should only work on correlated dimensions
SELECT * from remove_dimension('sample_table', 'time');
SELECT * from remove_dimension('sample_table', 'cpu');
\set ON_ERROR_STOP 1
-- Check that a DROP COLUMN removes entries from catalogs as well
SELECT * FROM add_dimension('sample_table', by_correlation('sensor_id'));
SELECT id AS dimension_id FROM _timescaledb_catalog.dimension WHERE type = 'r' \gset
ALTER TABLE sample_table DROP COLUMN sensor_id;
SELECT * FROM _timescaledb_catalog.dimension WHERE type = 'r';
SELECT * FROM _timescaledb_catalog.dimension_slice WHERE dimension_id = :dimension_id;

DROP TABLE sample_table;

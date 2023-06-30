-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Prepare test data for continuous aggregate size function tests
CREATE TABLE hypersize(time timestamptz, device int);
SELECT * FROM create_hypertable('hypersize', 'time');
INSERT INTO hypersize VALUES('2021-02-25', 1);

-- Test size functions on empty continuous aggregate
CREATE MATERIALIZED VIEW hypersize_cagg WITH (timescaledb.continuous) AS SELECT time_bucket('1 day', "time") AS bucket, AVG(device) AS value FROM hypersize GROUP BY 1 WITH NO DATA;
SELECT format('%I.%I', h.schema_name, h.table_name) AS "MAT_HYPERTABLE_NAME"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'hypersize_cagg'
\gset

SELECT * FROM hypertable_size(:'MAT_HYPERTABLE_NAME');
SELECT * FROM hypertable_detailed_size(:'MAT_HYPERTABLE_NAME') ORDER BY node_name;
SELECT * FROM chunks_detailed_size(:'MAT_HYPERTABLE_NAME') ORDER BY node_name;
SELECT * FROM hypertable_size('hypersize_cagg');
SELECT * FROM hypertable_detailed_size('hypersize_cagg') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('hypersize_cagg') ORDER BY node_name;

-- Test size functions on non-empty countinuous aggregate
CALL refresh_continuous_aggregate('hypersize_cagg', NULL, NULL);
SELECT * FROM hypertable_size('hypersize_cagg');
SELECT * FROM hypertable_detailed_size('hypersize_cagg') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('hypersize_cagg') ORDER BY node_name;
SELECT * FROM hypertable_size(:'MAT_HYPERTABLE_NAME');
SELECT * FROM hypertable_detailed_size(:'MAT_HYPERTABLE_NAME') ORDER BY node_name;
SELECT * FROM chunks_detailed_size(:'MAT_HYPERTABLE_NAME') ORDER BY node_name;

-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE extra_devices(device_id INT, name TEXT);
INSERT INTO extra_devices VALUES (4,'Device 4'),(5,'Device 5');
--add a device id that is not in metrics or metrics_compressed
-- need this for outer joins
INSERT INTO extra_devices VALUES ( 333,'Device New 333');

CREATE VIEW all_devices AS
SELECT * FROM devices
UNION
SELECT * FROM extra_devices;

-- Compressed CAgg + Compressed Hypertable
CREATE MATERIALIZED VIEW metrics_compressed_summary
WITH (timescaledb.continuous) AS
SELECT
	device_id,
	time_bucket(INTERVAL '1 week', time) AS bucket,
	AVG(v2),
	MIN(v2),
	MAX(v2)
FROM
	metrics_compressed
GROUP BY
	device_id, bucket
WITH NO DATA;

-- Force more than one chunk in the materialized hypertable
SELECT
    set_chunk_time_interval(
        format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name),
        '1 week'::interval
    )
FROM
    timescaledb_information.continuous_aggregates
WHERE
    view_name = 'metrics_compressed_summary';

ALTER MATERIALIZED VIEW metrics_compressed_summary SET (timescaledb.compress);
CALL refresh_continuous_aggregate('metrics_compressed_summary', NULL, '2000-01-15 23:55:00+0');

SELECT CASE WHEN res is NULL THEN NULL
            ELSE 'compressed'
       END as comp
FROM
( SELECT compress_chunk(show_chunks('metrics_compressed_summary')) res ) q;

-- Check for realtime caggs
SELECT
    count(*)
FROM
    metrics_compressed_summary
WHERE
    bucket > _timescaledb_internal.to_timestamp(
        _timescaledb_internal.cagg_watermark(
            (SELECT
                mat_hypertable_id
             FROM
                _timescaledb_catalog.continuous_agg
             WHERE
                user_view_name = 'metrics_compressed_summary')
        )
    );

-- Compressed CAgg + Uncompressed Hypertable
CREATE MATERIALIZED VIEW metrics_summary
WITH (timescaledb.continuous) AS
SELECT
	device_id,
	time_bucket(INTERVAL '1 week', time) AS bucket,
	AVG(v2),
	MIN(v2),
	MAX(v2)
FROM
	metrics
GROUP BY
	device_id, bucket
WITH NO DATA;

-- Force more than one chunk in the materialized hypertable
SELECT
    set_chunk_time_interval(
        format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name),
        '1 week'::interval
    )
FROM
    timescaledb_information.continuous_aggregates
WHERE
    view_name = 'metrics_summary';

ALTER MATERIALIZED VIEW metrics_summary SET (timescaledb.compress);
CALL refresh_continuous_aggregate('metrics_summary', NULL, '2000-01-15 23:55:00+0');
SELECT CASE WHEN res is NULL THEN NULL
            ELSE 'compressed'
       END as comp
FROM
( SELECT compress_chunk(show_chunks('metrics_summary')) res ) q;

-- Check for realtime caggs
SELECT
    count(*)
FROM
    metrics_summary
WHERE
    bucket > _timescaledb_internal.to_timestamp(
        _timescaledb_internal.cagg_watermark(
            (SELECT
                mat_hypertable_id
             FROM
                _timescaledb_catalog.continuous_agg
             WHERE
                user_view_name = 'metrics_summary')
        )
    );


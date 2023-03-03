-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE sensor_data(
time timestamptz not null,
sensor_id integer not null,
cpu double precision null,
temperature double precision null);

SELECT from create_hypertable('sensor_data','time');

INSERT INTO sensor_data
SELECT time + (INTERVAL '1 minute' * random()) AS time,
       sensor_id,
       random() AS cpu,
       random() * 100 AS temperature
FROM
       generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') AS g1(time),
       generate_series(1, 100, 1 ) AS g2(sensor_id)
ORDER BY time;

CREATE materialized VIEW sensor_data_v WITH(timescaledb.continuous) AS
SELECT sensor_id, time_bucket(INTERVAL '1 day', time) AS bucket,
AVG(temperature) FROM sensor_data
GROUP BY sensor_id, bucket;

INSERT INTO sensor_data
SELECT time + (INTERVAL '1 minute' * random()) AS time,
       sensor_id, random() AS cpu, random()* 100 AS temperature
FROM
       generate_series('2018-03-03 1:00'::TIMESTAMPTZ, '2018-03-31 1:00', '1 hour') AS g1(time),
       generate_series(1, 100, 1 ) AS g2(sensor_id)
ORDER BY time;

SELECT count(*) AS num_chunks FROM show_chunks('sensor_data');

SELECT drop_chunks('sensor_data','2018-03-28'::timestamp);

SELECT count(*) AS num_chunks from timescaledb_information.chunks where hypertable_name = 'sensor_data';

SELECT num_chunks from timescaledb_information.hypertables where hypertable_name = 'sensor_data';

SELECT count(*) AS num_chunks FROM show_chunks('sensor_data');

DROP TABLE sensor_data CASCADE;

--Prerequisites
CREATE TABLE conditions(
  day DATE NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL,
device_id int NOT NULL);
SELECT create_hypertable(
  'conditions', 'day',
  chunk_time_interval => INTERVAL '1 day'
);
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-14', 'Moscow', 26,1),
  ('2021-06-15', 'Moscow', 22,2),
  ('2021-06-16', 'Moscow', 24,3),
  ('2021-06-17', 'Moscow', 24,4),
  ('2021-06-18', 'Moscow', 27,4),
  ('2021-06-19', 'Moscow', 28,4),
  ('2021-06-20', 'Moscow', 30,1),
  ('2021-06-21', 'Moscow', 31,1),
  ('2021-06-22', 'Moscow', 34,1),
  ('2021-06-23', 'Moscow', 34,2),
  ('2021-06-24', 'Moscow', 34,2),
  ('2021-06-25', 'Moscow', 32,3),
  ('2021-06-26', 'Moscow', 32,3),
  ('2021-06-27', 'Moscow', 31,3);

CREATE TABLE devices ( device_id int not null, name text, location text);
INSERT INTO devices VALUES (1, 'thermo_1', 'Moscow'),
                           (2, 'thermo_2', 'Berlin'),
                           (3, 'thermo_3', 'London'),
                           (4, 'thermo_4', 'Stockholm');

--Simple join view
CREATE MATERIALIZED VIEW join_view AS
SELECT time_bucket('1 day'::interval, conditions.day) AS day,
    avg(conditions.temperature) AS avg,
    max(conditions.temperature) AS max,
    min(conditions.temperature) AS min,
    devices.name
  FROM conditions, devices
  WHERE conditions.device_id = devices.device_id
  GROUP BY devices.name, (time_bucket('1 day'::interval, conditions.day));

CREATE TABLE join_mat_ht AS SELECT * FROM join_view WITH NO DATA;
SELECT create_hypertable('join_mat_ht', 'day');
SELECT timescaledb_experimental.execute_materialization('join_view',
                                                        'join_mat_ht',
                                                        'conditions',
                                                        'day',
                                                        '2021-05-15',
                                                        '2022-12-31');
SELECT * FROM join_mat_ht;
INSERT INTO conditions VALUES ('2022-01-15', 'Stuttgart', 11,5),
                              ('2022-01-31', 'Stuttgart', 18,5),
                              ('2022-01-15', 'Barcelona', 21,6),
                              ('2022-01-31', 'Barcelona', 24,6);
INSERT INTO devices VALUES (5, 'thermo_5', 'Stuttgart'),
                           (6, 'thermo_6', 'Barcelona');

--Find the max materialized value and materialize from there
SELECT timescaledb_experimental.get_materialization_threshold('join_mat_ht',
                                                              'day') AS prev_mat_thresh;
\gset
SELECT timescaledb_experimental.get_materialization_threshold('conditions',
                                                              'day') AS target_mat_thresh;
\gset
SELECT timescaledb_experimental.execute_materialization('join_view',
                                                        'join_mat_ht',
                                                        'conditions',
                                                        'day',
                                                        :'prev_mat_thresh',
                                                        :'target_mat_thresh');

SELECT * FROM join_mat_ht;

--Drop objects
DROP TABLE conditions CASCADE;
DROP TABLE devices CASCADE;
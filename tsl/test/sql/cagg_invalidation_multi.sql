-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Disable background workers since we are testing manual refresh
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT _timescaledb_functions.stop_background_workers();
SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

SELECT _timescaledb_functions.invalidation_plugin_name() AS plugin_name \gset

CREATE VIEW hypertable_invalidation_thresholds AS
SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
       watermark AS threshold
  FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
  JOIN _timescaledb_catalog.hypertable ht
    ON hypertable_id = ht.id;

CREATE VIEW mat_invals AS
SELECT ca.user_view_name AS aggregate_name,
       ht.table_name,
       lowest_modified_value,
       greatest_modified_value
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.continuous_aggs_materialization_invalidation_log ml
    ON ca.mat_hypertable_id = ml.materialization_id
  JOIN _timescaledb_catalog.hypertable ht
    ON materialization_id = ht.id;


CREATE VIEW invalidation_slots AS
SELECT replace(slot_name::text, dboid::text, 'DBOID') AS slot_name,
       slot_type,
       database
 FROM pg_replication_slots,
      (select oid from pg_database where current_database() = datname) t(dboid)
WHERE plugin = :'plugin_name';

CREATE TABLE conditions (time bigint NOT NULL, device int, temp float);
SELECT create_hypertable('conditions', 'time', chunk_time_interval => 10);

CREATE TABLE measurements (time int NOT NULL, device int, temp float);
SELECT create_hypertable('measurements', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION bigint_now() RETURNS bigint LANGUAGE SQL STABLE
AS $$ SELECT coalesce(max(time), 0) FROM conditions $$;

CREATE OR REPLACE FUNCTION int_now() RETURNS int LANGUAGE SQL STABLE
AS $$ SELECT coalesce(max(time), 0) FROM measurements $$;

SELECT set_integer_now_func('conditions', 'bigint_now');
SELECT set_integer_now_func('measurements', 'int_now');

INSERT INTO conditions
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::int,
       abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

CREATE TABLE temp AS SELECT * FROM conditions;
INSERT INTO measurements SELECT * FROM temp;

-- Show the most recent data
SELECT * FROM conditions ORDER BY time DESC, device LIMIT 10;

SELECT * FROM invalidation_slots;

-- Create two continuous aggregates on the same hypertable to test
-- that invalidations are handled correctly across both of them.
-- set client_min_messages to debug2;
CREATE MATERIALIZED VIEW cond_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only = true,
      timescaledb.invalidate_using = 'wal')
AS
    SELECT time_bucket(BIGINT '10', time) AS bucket,
           device, avg(temp) AS avg_temp
      FROM conditions
    GROUP BY 1,2;

CREATE MATERIALIZED VIEW cond_20
WITH (timescaledb.continuous,
      timescaledb.materialized_only = true,
      timescaledb.invalidate_using = 'wal')
AS
SELECT time_bucket(BIGINT '20', time) AS bucket,
       device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2;

CREATE MATERIALIZED VIEW measure_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true,
      timescaledb.invalidate_using = 'wal')
AS
SELECT time_bucket(10, time) AS bucket,
       device,
       avg(temp) AS avg_temp
FROM measurements
GROUP BY 1,2;

-- There should be three continuous aggregates, two on one hypertable
-- and one on the other. All using the WAL.
SELECT hypertable_name, view_name, materialization_hypertable_name, invalidate_using
  FROM timescaledb_information.continuous_aggregates;

-- We need to refresh to move the invalidation threshold, or
-- invalidations will not be generated. Check initial value of
-- thresholds and materialization invalidations.
SELECT * FROM hypertable_invalidation_thresholds ORDER BY 1,2;

SELECT * INTO saved_invals FROM mat_invals;

INSERT INTO conditions VALUES
       (10, 4, 23.7), (10, 5, 23.8), (19, 3, 23.6), (60, 3, 23.7),
       (70, 4, 23.7);
INSERT INTO measurements VALUES
       (20, 4, 23.7), (30, 5, 23.8), (80, 3, 23.6);
INSERT INTO measurements VALUES
       (20, 4, 23.7), (140, 5, 23.8), (200, 3, 23.6);

SELECT * FROM invalidation_slots;
CALL _timescaledb_functions.process_hypertable_invalidations(
    ARRAY['measurements', 'conditions']
);

SELECT m.*
  FROM mat_invals m FULL JOIN saved_invals s ON row(m.*) = row(s.*)
 WHERE m.table_name IS NULL OR s.table_name IS NULL
ORDER BY 1,2,3;

DROP TABLE saved_invals;

-- Insert a lot of invalidations (more than 256) to trigger the code
-- that grows the ranges array and make sure that works as well. We
-- need to generate separate transactions for each insert, so we do
-- this using a loop rather than a single insert.

SELECT * FROM hypertable_invalidation_thresholds ORDER BY 1,2;

DO $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
     SELECT t, d FROM generate_series(1,100) t, generate_series(1,100) d
  LOOP
     INSERT INTO conditions VALUES (rec.t, rec.d, 100.0 * random());
     COMMIT;
  END LOOP;
END
$$;

SELECT * INTO saved_invals FROM mat_invals;

-- Peek at the WAL to check what we've got recorded.
SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'ts%cagg' \gset
SELECT count(data)
  FROM pg_logical_slot_peek_binary_changes(:'slot_name', NULL, 1000, variadic ARRAY['conditions', 'time']);

-- We set the work memory quite low to trigger range shedding when
-- running out of memory.
SET timescaledb.cagg_processing_low_work_mem TO 64;
SET timescaledb.cagg_processing_high_work_mem TO 96;
CALL _timescaledb_functions.process_hypertable_invalidations(ARRAY['conditions']);

-- Here we should get invalidations aligned to the bucket widths based
-- on the invalidation above.
--
-- We collect all the ranges into a multirange to avoid flakes because
-- of additional entries as a result of shedding below. The complete
-- invalidated ranges should be the same regardless.
SELECT m.aggregate_name,
       unnest(range_agg(multirange(int8range(m.lowest_modified_value, m.greatest_modified_value))))
  FROM mat_invals m FULL JOIN saved_invals s ON row(m.*) = row(s.*)
 WHERE m.table_name IS NULL OR s.table_name IS NULL
GROUP BY 1 ORDER BY 1,2;


\set ON_ERROR_STOP 1

\o /dev/null
\ir include/create_clustered_db.sql

\o
\set ECHO ALL
\c Test1

-- Utility function for grouping/slotting time with a given interval.
CREATE OR REPLACE FUNCTION date_group(
    field           timestamp,
    group_interval  interval
)
    RETURNS timestamp LANGUAGE SQL STABLE AS
$BODY$
    SELECT to_timestamp((EXTRACT(EPOCH from $1)::int /
        EXTRACT(EPOCH from group_interval)::int) *
        EXTRACT(EPOCH from group_interval)::int)::timestamp;
$BODY$;

CREATE TABLE PUBLIC."testNs" (
  "timeCustom" TIMESTAMP NOT NULL,
  device_id TEXT NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL,
  series_bool BOOLEAN NULL
);
CREATE INDEX ON PUBLIC."testNs" (device_id, "timeCustom" DESC NULLS LAST) WHERE device_id IS NOT NULL;
SELECT * FROM create_hypertable('"public"."testNs"', 'timeCustom', 'device_id', associated_schema_name=>'testNs' );


\c Test1
INSERT INTO PUBLIC."testNs"("timeCustom", device_id, series_0, series_1) VALUES
('2009-11-12T01:00:00+00:00', 'dev1', 1.5, 1),
('2009-11-12T01:00:00+00:00', 'dev1', 1.5, 2),
('2009-11-10T23:00:02+00:00', 'dev1', 2.5, 3);

SELECT _iobeamdb_meta_api.close_chunk_end_immediate(c.id)
FROM get_open_partition_for_key((SELECT id FROM _iobeamdb_catalog.hypertable WHERE table_name = 'testNs'), 'dev1') part
INNER JOIN _iobeamdb_catalog.chunk c ON (c.partition_id = part.id);

INSERT INTO PUBLIC."testNs"("timeCustom", device_id, series_0, series_1) VALUES
('2009-11-10T23:00:00+00:00', 'dev2', 1.5, 1),
('2009-11-10T23:00:00+00:00', 'dev2', 1.5, 2);

SELECT * FROM PUBLIC."testNs";

SET client_min_messages = WARNING;

\echo 'The next 2 queries will differ in output between UTC and EST since the mod is on the 100th hour UTC'
SET timezone = 'UTC';
SELECT date_group("timeCustom", '100 days') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

SET timezone = 'EST';
SELECT date_group("timeCustom", '100 days') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

\echo 'The rest of the queries will be the same in output between UTC and EST'
--have to set the timezones on both Test1 and test2. Have to also kill ongoing dblinks as their sessions cache the timezone setting.
SET timezone = 'UTC';
ALTER DATABASE test2 SET timezone ='UTC';
SELECT dblink_disconnect(conn) FROM unnest(dblink_get_connections()) conn;
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

SET timezone = 'EST';
ALTER DATABASE test2 SET timezone ='EST';
SELECT dblink_disconnect(conn) FROM unnest(dblink_get_connections()) conn;
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC;

SET timezone = 'UTC';
ALTER DATABASE test2 SET timezone ='UTC';
SELECT dblink_disconnect(conn) FROM unnest(dblink_get_connections()) conn;

SELECT *
FROM PUBLIC."testNs"
WHERE "timeCustom" >= TIMESTAMP '2009-11-10T23:00:00'
AND "timeCustom" < TIMESTAMP '2009-11-12T01:00:00' ORDER BY "timeCustom" DESC;

SET timezone = 'EST';
ALTER DATABASE test2 SET timezone ='EST';
SELECT dblink_disconnect(conn) FROM unnest(dblink_get_connections()) conn;
SELECT *
FROM PUBLIC."testNs"
WHERE "timeCustom" >= TIMESTAMP '2009-11-10T23:00:00'
AND "timeCustom" < TIMESTAMP '2009-11-12T01:00:00' ORDER BY "timeCustom" DESC;

SET timezone = 'UTC';
ALTER DATABASE test2 SET timezone ='UTC';
SELECT dblink_disconnect(conn) FROM unnest(dblink_get_connections()) conn;
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC LIMIT 2;

SET timezone = 'EST';
ALTER DATABASE test2 SET timezone ='EST';
SELECT dblink_disconnect(conn) FROM unnest(dblink_get_connections()) conn;
SELECT date_group("timeCustom", '1 day') AS time, sum(series_0)
FROM PUBLIC."testNs" GROUP BY time ORDER BY time ASC LIMIT 2;

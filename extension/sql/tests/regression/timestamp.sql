\set ON_ERROR_STOP 1

\o /dev/null
\ir include/create_clustered_db.sql

\o 
\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

\c Test1

CREATE TABLE PUBLIC."testNs" (
  "timeCustom" TIMESTAMP NOT NULL,
  device_id TEXT NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL,
  series_bool BOOLEAN NULL
);
CREATE INDEX ON PUBLIC."testNs" (device_id, "timeCustom" DESC NULLS LAST) WHERE device_id IS NOT NULL;
SELECT * FROM create_hypertable('"public"."testNs"', 'timeCustom', 'device_id', hypertable_name=>'testNs', associated_schema_name=>'testNs' );

SELECT set_is_distinct_flag('"public"."testNs"', 'device_id', TRUE);


\c Test1
INSERT INTO "testNs"("timeCustom", device_id, series_0, series_1) VALUES
('2009-11-12T01:00:00+00:00', 'dev1', 1.5, 1),
('2009-11-12T01:00:00+00:00', 'dev1', 1.5, 2),
('2009-11-10T23:00:02+00:00', 'dev1', 2.5, 3);

SELECT close_chunk_end(c.id)
FROM get_open_partition_for_key('testNs', 'dev1') part
INNER JOIN chunk c ON (c.partition_id = part.id);

INSERT INTO "testNs"("timeCustom", device_id, series_0, series_1) VALUES
('2009-11-10T23:00:00+00:00', 'dev2', 1.5, 1),
('2009-11-10T23:00:00+00:00', 'dev2', 1.5, 2);

SELECT * FROM PUBLIC."testNs";


SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs'));

\echo 'The next 2 queries will differ in output between UTC and EST since the mod is on the 100th hour UTC'
SET timezone = 'UTC';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e6) :: BIGINT)
                     ));
SET timezone = 'EST';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((100 * 60 * 60 * 1e6) :: BIGINT)
                     ));

\echo 'The rest of the queries will be the same in output between UTC and EST'

SET timezone = 'UTC';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((1 * 60 * 60 * 1e6) :: BIGINT)
                     ));
SET timezone = 'EST';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((1 * 60 * 60 * 1e6) :: BIGINT)
                     ));



SET timezone = 'UTC';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((1 * 60 * 60 * 1e6) :: BIGINT)
                     ));
SET timezone = 'EST';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((1 * 60 * 60 * 1e6) :: BIGINT)
                     ));


SET timezone = 'UTC';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    time_condition=>new_time_condition(1257894000000000,1257987600000000) --microseconds
                  ));
SET timezone = 'EST';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    time_condition=>new_time_condition(1257894000000000,1257987600000000) --microseconds
                  ));


SET timezone = 'UTC';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((1 * 60 * 60 * 1e6) :: BIGINT),
                                    limit_time_periods => 2 
                  ));
SET timezone = 'EST';
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate => new_aggregate((1 * 60 * 60 * 1e6) :: BIGINT),
                                    limit_time_periods => 2
                  ));

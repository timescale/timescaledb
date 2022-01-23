-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TYPE custom_type AS (high int, low int);

CREATE TABLE conditions_before (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null,
      highlow     custom_type null,
      bit_int     smallint,
      good_life   boolean
    );

SELECT table_name FROM create_hypertable( 'conditions_before', 'timec');

INSERT INTO conditions_before
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO conditions_before
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO conditions_before
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL, NULL, 8, true;

CREATE TABLE conditions_after (like conditions_before including all);
SELECT table_name FROM create_hypertable( 'conditions_after', 'timec');
INSERT INTO conditions_after SELECT * FROM conditions_before;

SELECT
  $$
  SELECT time_bucket('1week', timec) as bucket,
    location,
    min(allnull) as min_allnull,
    max(temperature) as max_temp,
    sum(temperature)+sum(humidity) as agg_sum_expr,
    avg(humidity),
    stddev(humidity),
    bit_and(bit_int),
    bit_or(bit_int),
    bool_and(good_life),
    every(temperature > 0),
    bool_or(good_life),
    count(*) as count_rows,
    count(temperature) as count_temp,
    count(allnull) as count_zero,
    corr(temperature, humidity),
    covar_pop(temperature, humidity),
    covar_samp(temperature, humidity),
    regr_avgx(temperature, humidity),
    regr_avgy(temperature, humidity),
    regr_count(temperature, humidity),
    regr_intercept(temperature, humidity),
    regr_r2(temperature, humidity),
    regr_slope(temperature, humidity),
    regr_sxx(temperature, humidity),
    regr_sxy(temperature, humidity),
    regr_syy(temperature, humidity),
    stddev(temperature) as stddev_temp,
    stddev_pop(temperature),
    stddev_samp(temperature),
    variance(temperature),
    var_pop(temperature),
    var_samp(temperature),
    last(temperature, timec) as last_temp,
    last(highlow, timec) as last_hl,
    first(highlow, timec) as first_hl,
    histogram(temperature, 0, 100, 5)
  FROM TABLE
  GROUP BY bucket, location
  HAVING min(location) >= 'NYC' and avg(temperature) > 20
  $$ AS "QUERY_TEMPLATE"
\gset

SELECT
  replace(:'QUERY_TEMPLATE', 'TABLE', 'conditions_before') AS "QUERY_BEFORE",
  replace(:'QUERY_TEMPLATE', 'TABLE', 'conditions_after') AS "QUERY_AFTER"
\gset


DROP MATERIALIZED VIEW IF EXISTS mat_test;

--materialize this VIEW before dump this tests
--that the partial state survives the dump intact
CREATE MATERIALIZED VIEW mat_before
WITH (timescaledb.continuous)
AS :QUERY_BEFORE WITH NO DATA;

--materialize this VIEW after dump this tests
--that the partialize VIEW and refresh mechanics
--survives the dump intact
CREATE MATERIALIZED VIEW mat_after
WITH (timescaledb.continuous)
AS :QUERY_AFTER WITH NO DATA;

--materialize mat_before

SET client_min_messages TO LOG;
CALL refresh_continuous_aggregate('mat_before', NULL, NULL);

SELECT count(*) FROM conditions_before;
SELECT count(*) FROM conditions_after;

SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_VIEW_name as "PART_VIEW_NAME",
       partial_VIEW_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_VIEW_name = 'mat_before'
\gset

SELECT count(*) FROM conditions_before;
SELECT count(*) FROM conditions_after;

--dump & restore
\c postgres :ROLE_SUPERUSER
\! utils/pg_dump_aux_dump.sh dump/pg_dump.sql

\c :TEST_DBNAME
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;

--\! cp dump/pg_dump.sql /tmp/dump.sql
SELECT timescaledb_pre_restore();
\! utils/pg_dump_aux_restore.sh dump/pg_dump.sql
SELECT timescaledb_post_restore();
SELECT _timescaledb_internal.stop_background_workers();

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--make sure the appropriate DROP are still blocked.
\set ON_ERROR_STOP 0
DROP table :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME";
DROP VIEW :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME";
\set ON_ERROR_STOP 1

--materialize mat_after
CALL refresh_continuous_aggregate('mat_after', NULL, NULL);
SELECT count(*) FROM mat_after;

--compare results
SELECT count(*) FROM conditions_before;
SELECT count(*) FROM conditions_after;
\set VIEW_NAME mat_before
\set QUERY :QUERY_BEFORE
\set ECHO errors
\ir include/cont_agg_test_equal.sql
\set ECHO all
\set VIEW_NAME mat_after
\set QUERY :QUERY_AFTER
\set ECHO errors
\ir include/cont_agg_test_equal.sql
\set ECHO all

DROP MATERIALIZED VIEW mat_after;
DROP MATERIALIZED VIEW mat_before;

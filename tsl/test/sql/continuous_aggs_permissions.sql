-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- initialize the bgw mock state to prevent the materialization workers from running
\c :TEST_DBNAME :ROLE_SUPERUSER

-- remove any default jobs, e.g., telemetry so bgw_job isn't polluted
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE conditions (
      timec       INT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test1() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0) FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_test1');


create or replace view mat_refresh_test
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '-200')
as
select location, max(humidity)
from conditions
group by time_bucket(100, timec), location;

insert into conditions
select generate_series(0, 50, 10), 'NYC', 55, 75, 40, 70, NULL;

REFRESH MATERIALIZED VIEW  mat_refresh_test;

SELECT id as cagg_job_id FROM _timescaledb_config.bgw_job order by id desc limit 1 \gset
SELECT materialization_hypertable FROM timescaledb_information.continuous_aggregates  WHERE view_name = 'mat_refresh_test'::regclass \gset 

SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'mat_refresh_test' \gset

SELECT schema_name as mat_chunk_schema, table_name as mat_chunk_table 
FROM _timescaledb_catalog.chunk 
WHERE hypertable_id = :mat_hypertable_id 
ORDER BY id desc
LIMIT 1 \gset

CREATE TABLE conditions_for_perm_check (
      timec       INT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable('conditions_for_perm_check', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test2() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0) FROM conditions_for_perm_check $$;
SELECT set_integer_now_func('conditions_for_perm_check', 'integer_now_test2');

CREATE TABLE conditions_for_perm_check_w_grant (
      timec       INT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable('conditions_for_perm_check_w_grant', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test3() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0) FROM conditions_for_perm_check_w_grant $$;
SELECT set_integer_now_func('conditions_for_perm_check_w_grant', 'integer_now_test3');

GRANT SELECT, TRIGGER ON conditions_for_perm_check_w_grant TO public;

insert into conditions_for_perm_check_w_grant
select generate_series(0, 30, 10), 'POR', 55, 75, 40, 70, NULL;

--need both select and trigger permissions to create a materialized view on top of it.
GRANT SELECT, TRIGGER ON conditions_for_perm_check_w_grant TO public;

\c  :TEST_DBNAME :ROLE_SUPERUSER

create schema custom_schema;

CREATE FUNCTION get_constant() RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT 10;
$BODY$;

REVOKE EXECUTE ON FUNCTION get_constant() FROM PUBLIC;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
select from alter_job_schedule(:cagg_job_id, max_runtime => NULL);

--make sure that commands fail

ALTER VIEW mat_refresh_test SET(timescaledb.refresh_lag = '6 h', timescaledb.refresh_interval = '2h');
ALTER VIEW mat_refresh_test SET(timescaledb.materialized_only = true);
DROP VIEW mat_refresh_test CASCADE; 
REFRESH MATERIALIZED VIEW mat_refresh_test;
SELECT * FROM mat_refresh_test;
SELECT * FROM :materialization_hypertable;
SELECT * FROM :"mat_chunk_schema".:"mat_chunk_table";

--cannot create a mat view without select and trigger grants
create or replace view mat_perm_view_test
WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select location, max(humidity)
from conditions_for_perm_check
group by time_bucket(100, timec), location;

--cannot create mat view in a schema without create privileges
create or replace view custom_schema.mat_perm_view_test
WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select location, max(humidity)
from conditions_for_perm_check_w_grant
group by time_bucket(100, timec), location;

--cannot use a function without EXECUTE privileges
--you can create a VIEW but cannot refresh it
create or replace view mat_perm_view_test
WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select location, max(humidity), get_constant()
from conditions_for_perm_check_w_grant
group by time_bucket(100, timec), location;

--this should fail
REFRESH MATERIALIZED VIEW mat_perm_view_test;
DROP VIEW mat_perm_view_test CASCADE;

--can create a mat view on something with select and trigger grants
create or replace view mat_perm_view_test
WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select location, max(humidity)
from conditions_for_perm_check_w_grant
group by time_bucket(100, timec), location;

REFRESH MATERIALIZED VIEW mat_perm_view_test;
SELECT * FROM mat_perm_view_test;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--revoke select permissions from role with mat view
REVOKE SELECT ON conditions_for_perm_check_w_grant FROM public;

insert into conditions_for_perm_check_w_grant
select generate_series(100, 130, 10), 'POR', 65, 85, 30, 90, NULL;

\c  :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
--refresh mat view should now fail due to lack of permissions
REFRESH MATERIALIZED VIEW mat_perm_view_test;

--but the old data will still be there
SELECT * FROM mat_perm_view_test;

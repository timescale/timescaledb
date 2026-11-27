-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE check_ht(time timestamptz NOT NULL, value int);
SELECT FROM create_hypertable('check_ht', 'time');
CREATE TABLE check_plain(a int);
CREATE VIEW check_view AS SELECT 1 AS a;

-- continuous aggregates
CREATE MATERIALIZED VIEW check_cagg WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket, count(*) FROM check_ht GROUP BY 1 WITH NO DATA;
CREATE VIEW check_other_view AS SELECT 1 AS a;
ALTER VIEW check_other_view OWNER TO :ROLE_DEFAULT_PERM_USER;
GRANT UPDATE ON _timescaledb_catalog.continuous_agg TO :ROLE_DEFAULT_PERM_USER;

SELECT conname FROM pg_constraint
WHERE conrelid = '_timescaledb_catalog.continuous_agg'::regclass AND contype = 'c';

-- valid values are accepted
BEGIN;
UPDATE _timescaledb_catalog.continuous_agg SET direct_view_schema = 'public', direct_view_name = 'check_view';
ROLLBACK;

\set ON_ERROR_STOP 0
-- invalid views
UPDATE _timescaledb_catalog.continuous_agg SET partial_view_name = 'check_missing';
UPDATE _timescaledb_catalog.continuous_agg SET partial_view_schema = 'pg_catalog', partial_view_name = 'pg_views';
UPDATE _timescaledb_catalog.continuous_agg SET direct_view_schema = 'public', direct_view_name = 'check_plain';
UPDATE _timescaledb_catalog.continuous_agg SET direct_view_schema = user_view_schema, direct_view_name = user_view_name;
-- view owned by a different role
UPDATE _timescaledb_catalog.continuous_agg SET partial_view_schema = 'public', partial_view_name = 'check_other_view';
-- materialization hypertable that is its own source
UPDATE _timescaledb_catalog.continuous_agg SET raw_hypertable_id = mat_hypertable_id;
-- a role can only write continuous aggregates of owners it has the privileges of
SET ROLE :ROLE_DEFAULT_PERM_USER;
UPDATE _timescaledb_catalog.continuous_agg SET materialized_only = true;
RESET ROLE;
\set ON_ERROR_STOP 1

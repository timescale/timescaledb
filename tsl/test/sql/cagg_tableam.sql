-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE ACCESS METHOD heap2 TYPE TABLE HANDLER heap_tableam_handler;

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE VIEW cagg_info AS
WITH
  caggs AS (
    SELECT format('%I.%I', user_view_schema, user_view_name)::regclass AS user_view,
           format('%I.%I', ht.schema_name, ht.table_name)::regclass AS mat_relid
      FROM _timescaledb_catalog.hypertable ht,
           _timescaledb_catalog.continuous_agg cagg
     WHERE ht.id = cagg.mat_hypertable_id
  )
SELECT user_view,
       relname AS mat_table,
       (SELECT spcname FROM pg_tablespace WHERE oid = reltablespace) AS tablespace
  FROM pg_class JOIN caggs ON pg_class.oid = caggs.mat_relid;

CREATE TABLE whatever(time BIGINT NOT NULL, data INTEGER);

SELECT hypertable_id AS whatever_nid
  FROM create_hypertable('whatever', 'time', chunk_time_interval => 10)
\gset

CREATE OR REPLACE FUNCTION integer_now_test() RETURNS bigint
LANGUAGE SQL STABLE AS $$
	SELECT coalesce(max(time), bigint '0') FROM whatever
$$;

SELECT set_integer_now_func('whatever', 'integer_now_test');

INSERT INTO whatever SELECT i, i FROM generate_series(0, 29) AS i;

-- Checking that the access method for the materialized hypertable is
-- set to the correct access method.
CREATE MATERIALIZED VIEW tableam_view USING heap2
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket('5', time), COUNT(data)
  FROM whatever GROUP BY 1 WITH NO DATA;

CREATE MATERIALIZED VIEW notableam_view
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket('5', time), COUNT(data)
  FROM whatever GROUP BY 1 WITH NO DATA;

SELECT user_view, mat_table, amname
FROM cagg_info,
     LATERAL (SELECT relam FROM pg_class WHERE relname = mat_table) AS cls,
     LATERAL (SELECT amname FROM pg_am WHERE oid = relam) AS am
WHERE user_view::text IN ('tableam_view', 'notableam_view');

-- Check that the view with the other access method actually works.
SELECT * FROM notableam_view ORDER BY 1;
SELECT * FROM tableam_view ORDER BY 1;
CALL refresh_continuous_aggregate('notableam_view', NULL, NULL);
CALL refresh_continuous_aggregate('tableam_view', NULL, NULL);
SELECT * FROM notableam_view ORDER BY 1;
SELECT * FROM tableam_view ORDER BY 1;

DROP MATERIALIZED VIEW notableam_view;
DROP MATERIALIZED VIEW tableam_view;

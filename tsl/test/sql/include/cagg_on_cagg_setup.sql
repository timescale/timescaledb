-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- CAGGs on CAGGs tests
\if :IS_DISTRIBUTED
\echo 'Running distributed hypertable tests'
\else
\echo 'Running local hypertable tests'
\endif

SET ROLE :ROLE_DEFAULT_PERM_USER;

DROP TABLE IF EXISTS conditions CASCADE;
\if :IS_DEFAULT_COLUMN_ORDER
  CREATE TABLE conditions (
    time :TIME_DIMENSION_DATATYPE NOT NULL,
    temperature NUMERIC
  );
\else
  CREATE TABLE conditions (
    temperature NUMERIC,
    time :TIME_DIMENSION_DATATYPE NOT NULL
  );
\endif

\if :IS_DISTRIBUTED
  \if :IS_TIME_DIMENSION
    SELECT table_name FROM create_distributed_hypertable('conditions', 'time', replication_factor => 2);
  \else
    SELECT table_name FROM create_distributed_hypertable('conditions', 'time', chunk_time_interval => 10, replication_factor => 2);
  \endif
\else
  \if :IS_TIME_DIMENSION
    SELECT table_name FROM create_hypertable('conditions', 'time');
  \else
    SELECT table_name FROM create_hypertable('conditions', 'time', chunk_time_interval => 10);
  \endif
\endif

\if :IS_TIME_DIMENSION
  INSERT INTO conditions ("time", temperature) VALUES ('2022-01-01 00:00:00-00', 10);
  INSERT INTO conditions ("time", temperature) VALUES ('2022-01-01 01:00:00-00',  5);
  INSERT INTO conditions ("time", temperature) VALUES ('2022-01-02 01:00:00-00', 20);
\else
  CREATE OR REPLACE FUNCTION integer_now()
  RETURNS :TIME_DIMENSION_DATATYPE LANGUAGE SQL STABLE AS
  $$
    SELECT coalesce(max(time), 0)
    FROM conditions
  $$;

  \if :IS_DISTRIBUTED
    SELECT
      'CREATE OR REPLACE FUNCTION integer_now() RETURNS '||:'TIME_DIMENSION_DATATYPE'||' LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM conditions $$;' AS "STMT"
      \gset
    CALL distributed_exec (:'STMT');
  \endif

  SELECT set_integer_now_func('conditions', 'integer_now');

  INSERT INTO conditions ("time", temperature) VALUES (1, 10);
  INSERT INTO conditions ("time", temperature) VALUES (2,  5);
  INSERT INTO conditions ("time", temperature) VALUES (5, 20);
\endif

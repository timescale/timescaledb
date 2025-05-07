-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ECHO errors
\set VERBOSITY default
\c :TEST_DBNAME :ROLE_SUPERUSER

\set TMP_USER :TEST_DBNAME _wizard

DO $$
BEGIN
  ASSERT( _timescaledb_functions.get_partition_for_key(''::text) = 669664877 );
  ASSERT( _timescaledb_functions.get_partition_for_key('dev1'::text) = 1129986420 );
  ASSERT( _timescaledb_functions.get_partition_for_key('longlonglonglongpartitionkey'::text) = 1169179734);
END$$;

\pset null '[NULL]'
CREATE USER :TMP_USER;
SELECT * FROM (
    VALUES
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert', false)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert,select', false)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(NULL, :'TMP_USER', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', NULL, 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', NULL, true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', :'TMP_USER', 'insert,select', NULL)),
       (_timescaledb_functions.makeaclitem(0, :'TMP_USER', 'insert,select', true)),
       (_timescaledb_functions.makeaclitem(:'TMP_USER', 0, 'insert,select', true))
    ) AS t(item);
DROP USER :TMP_USER;

CREATE TABLE data (vid serial, rng text);
INSERT INTO data(rng)
VALUES ('["2025-04-25 11:10:00+02","2025-04-25 11:14:00+02"]'),
       ('["2025-04-25 11:10:00+02","2025-04-25 11:17:00+02"]'),
       ('["2025-04-25 11:10:00+02","2025-04-25 11:20:00+02")');

\set ECHO all
SELECT _timescaledb_functions.align_to_bucket('5 minutes'::interval, rng::tstzrange) FROM data;

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.align_to_bucket(null, null);
SELECT _timescaledb_functions.align_to_bucket(null::interval, null::tstzrange);
SELECT _timescaledb_functions.align_to_bucket(
       null::interval,
       '["2025-04-25 11:10:00+02","2025-04-25 11:14:00+02"]'::tstzrange
);
\set ON_ERROR_STOP 1

SELECT typ,
       _timescaledb_functions.get_internal_time_min(typ),
       _timescaledb_functions.get_internal_time_max(typ)
  FROM (VALUES
    ('bigint'::regtype), ('int'::regtype), ('smallint'::regtype),
    ('timestamp'::regtype), ('timestamptz'::regtype), ('date'::regtype),
    (null::regtype)
  ) t(typ);

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.get_internal_time_min(0);
SELECT _timescaledb_functions.get_internal_time_max(0);
\set ON_ERROR_STOP 1

WITH
  tstzranges AS (
    SELECT vid,
           rng::tstzrange,
           lower(rng::tstzrange) AS lower_ts,
           upper(rng::tstzrange) AS upper_ts
      FROM data
  ),
  usecranges AS (
    SELECT vid,
           _timescaledb_functions.to_unix_microseconds(lower_ts) AS lower_usec,
           _timescaledb_functions.to_unix_microseconds(upper_ts) AS upper_usec
      FROM tstzranges
  )
SELECT _timescaledb_functions.make_multirange_from_internal_time(rng, lower_usec, upper_usec),
       _timescaledb_functions.make_range_from_internal_time(rng, lower_ts, upper_ts)
  FROM tstzranges join usecranges using (vid);

WITH
  tsranges AS (
    SELECT vid,
           rng::tsrange,
           lower(rng::tsrange) AS lower_ts,
           upper(rng::tsrange) AS upper_ts
      FROM data
  ),
  usecranges AS (
    SELECT vid,
           _timescaledb_functions.to_unix_microseconds(lower_ts) AS lower_usec,
           _timescaledb_functions.to_unix_microseconds(upper_ts) AS upper_usec
      FROM tsranges
  )
SELECT _timescaledb_functions.make_multirange_from_internal_time(rng, lower_usec, upper_usec),
       _timescaledb_functions.make_range_from_internal_time(rng, lower_ts, upper_ts)
  FROM tsranges join usecranges using (vid);

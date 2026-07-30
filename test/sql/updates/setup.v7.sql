-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir setup.catalog.sql
\ir setup.bigint.sql
\ir setup.constraints.sql
\ir setup.insert_bigint.v2.sql
\ir setup.timestamp.sql

ALTER TABLE PUBLIC.hyper_timestamp
  ADD CONSTRAINT exclude_const
  EXCLUDE USING btree (
        "time" WITH =, device_id WITH =
   ) WHERE (value > 0);

\ir setup.insert_timestamp.sql
\ir setup.drop_meta.sql
\ir setup.continuous_aggs.sql
\ir setup.compression.sql
\ir setup.policies.sql

-- Space-partitioned hypertable for extra update/downgrade coverage
CREATE TABLE space_constraints (
  time timestamptz NOT NULL,
  device int NOT NULL,
  value double precision
);

SELECT create_hypertable('space_constraints', 'time', 'device',
                         number_partitions => 2,
                         chunk_time_interval => interval '1 day');

INSERT INTO space_constraints
SELECT '2020-01-01'::timestamptz + (g % 6) * interval '1 day', g % 4, g
FROM generate_series(1, 200) g;

-- Drop a couple of chunks.
SELECT drop_chunks('space_constraints', '2020-01-03'::timestamptz);

INSERT INTO space_constraints
SELECT '2020-02-01'::timestamptz + (g % 5) * interval '1 day', g % 4, g
FROM generate_series(1, 200) g;

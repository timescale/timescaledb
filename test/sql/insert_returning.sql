-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE standard_table (
    standard_id integer PRIMARY KEY,
    name text not null
);

CREATE TABLE hypertable (
    time timestamptz not null,
    name text not null,
    standard_id integer not null
);

select * from create_hypertable('hypertable', 'time');

INSERT INTO standard_table (standard_id, name)
VALUES (1, 'standard_1');

INSERT INTO hypertable (time, name, standard_id)
VALUES ('2021-01-01 01:01:01+00', 'hypertable_1', 1);

INSERT INTO hypertable (time, name, standard_id)
VALUES ('2022-02-02 02:02:02+00', 'hypertable_2', 1)
RETURNING *, EXISTS (
  SELECT *
  FROM standard_table
  WHERE standard_table.standard_id = hypertable.standard_id
);

INSERT INTO hypertable (time, name, standard_id)
VALUES ('2023-03-03 03:03:03+00', 'hypertable_3', 1)
RETURNING *, EXISTS (
  SELECT *
  FROM standard_table
  WHERE standard_table.standard_id = hypertable.standard_id
);

INSERT INTO hypertable (time, name, standard_id)
VALUES ('2024-04-04 04:04:04+00', 'hypertable_4', 2)
RETURNING *, EXISTS (
  SELECT *
  FROM standard_table
  WHERE standard_table.standard_id = hypertable.standard_id
);
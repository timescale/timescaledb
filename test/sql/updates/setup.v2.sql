-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

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

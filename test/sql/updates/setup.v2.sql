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

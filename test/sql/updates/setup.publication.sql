-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Hypertable with pre-upgrade chunks for the schema-publication backfill
-- check in post.publication.sql.
CREATE SCHEMA update_pub_schema;
CREATE TABLE update_pub_schema.up_pub_ht (time timestamptz NOT NULL, device_id int, value float);
SELECT create_hypertable('update_pub_schema.up_pub_ht', 'time');
INSERT INTO update_pub_schema.up_pub_ht VALUES ('2020-01-01 00:00:00+00', 1, 1.0);
INSERT INTO update_pub_schema.up_pub_ht VALUES ('2020-01-08 00:00:00+00', 2, 2.0);

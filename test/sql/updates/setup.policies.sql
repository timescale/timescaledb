-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE policy_test_timestamptz (
    time timestamptz,
    device_id int,
    value float
);

SELECT table_name FROM create_hypertable ('policy_test_timestamptz', 'time');

SELECT * FROM
    add_drop_chunks_policy ('policy_test_timestamptz', '60d'::interval,
        cascade_to_materializations => FALSE);

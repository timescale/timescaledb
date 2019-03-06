-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create a table where we create chunks in order
CREATE TABLE ordered_append(time timestamptz NOT NULL, device_id INT, value float);
SELECT create_hypertable('ordered_append','time');
CREATE index on ordered_append(time DESC,device_id);
CREATE index on ordered_append(device_id,time DESC);

INSERT INTO ordered_append VALUES('2000-01-01',1,1.0);
INSERT INTO ordered_append VALUES('2000-01-08',1,2.0);
INSERT INTO ordered_append VALUES('2000-01-15',1,3.0);

-- create a second table where we create chunks in reverse order
CREATE TABLE ordered_append_reverse(time timestamptz NOT NULL, device_id INT, value float);
SELECT create_hypertable('ordered_append_reverse','time');

INSERT INTO ordered_append_reverse VALUES('2000-01-15',1,1.0);
INSERT INTO ordered_append_reverse VALUES('2000-01-08',1,2.0);
INSERT INTO ordered_append_reverse VALUES('2000-01-01',1,3.0);


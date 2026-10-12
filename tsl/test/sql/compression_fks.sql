-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test	foreign key constraints with compression
CREATE TABLE keys(time timestamptz unique);
CREATE TABLE ht_with_fk(time timestamptz);
SELECT create_hypertable('ht_with_fk','time');

ALTER TABLE ht_with_fk ADD CONSTRAINT keys FOREIGN KEY (time) REFERENCES keys(time) ON DELETE CASCADE;
ALTER TABLE ht_with_fk SET (timescaledb.compress,timescaledb.compress_segmentby='time');

-- no keys added yet so any insert into ht_with_fk should fail
\set ON_ERROR_STOP 0
INSERT INTO ht_with_fk SELECT '2000-01-01';
\set ON_ERROR_STOP 1

-- create a key in the referenced table
INSERT INTO keys SELECT '2000-01-01';

-- now the insert should succeed
INSERT INTO ht_with_fk SELECT '2000-01-01';

SELECT compress_chunk(ch) FROM show_chunks('ht_with_fk') ch;

-- insert should still succeed after compression
INSERT INTO ht_with_fk SELECT '2000-01-01';

-- inserting key not present in keys should fail
\set ON_ERROR_STOP 0
INSERT INTO ht_with_fk SELECT '2000-01-01 0:00:01';
\set ON_ERROR_STOP 1

SELECT conrelid::regclass,conname,confrelid::regclass FROM pg_constraint WHERE contype = 'f' AND confrelid = 'keys'::regclass ORDER BY conrelid::regclass::text COLLATE "C",conname;


-- compress hypertable referenced by a foreign key
CREATE TABLE ht_referenced(time timestamptz NOT NULL, id int, UNIQUE(time, id));
SELECT create_hypertable('ht_referenced', 'time');
CREATE TABLE ht_referencing(time timestamptz, id int, FOREIGN KEY(time, id) REFERENCES ht_referenced(time, id));
INSERT INTO ht_referenced VALUES ('2025-01-01', 1), ('2025-04-01', 2);
INSERT INTO ht_referencing VALUES ('2025-01-01', 1);
ALTER TABLE ht_referenced SET (timescaledb.compress, timescaledb.compress_segmentby = 'id');

SELECT count(compress_chunk(ch)) FROM show_chunks('ht_referenced') ch;
SELECT * FROM ht_referenced ORDER BY time;

-- foreign key is still enforced after compression
INSERT INTO ht_referencing VALUES ('2025-04-01', 2);
\set ON_ERROR_STOP 0
INSERT INTO ht_referencing VALUES ('2025-04-01', 3);
DELETE FROM ht_referenced WHERE id = 1;
\set ON_ERROR_STOP 1
SELECT * FROM ht_referencing ORDER BY time;

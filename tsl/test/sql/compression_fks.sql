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


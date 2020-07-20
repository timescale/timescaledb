# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# Race condition between insert and drop_chunks
#
# If an insert need to create a new chunk, it will look for existing
# dimension slices to see if any need to be added: if slices already
# exist, they do not need to be re-constructed and constraints can be
# added that reference these slices. If chunks are dropped, there is a
# cleanup of unreferenced dimension slices which can possibly remove
# unreferenced dimension slices if transactions creating new chunks do
# not lock the dimension slices for read.
#
# This isolation test check that a concurrent insert and drop_chunks
# do not accidentally create a broken state by adding chunk
# constraints that reference non-existing dimension slices.

setup {
  DROP TABLE IF EXISTS insert_dropchunks_race_t1;
  CREATE TABLE insert_dropchunks_race_t1 (time timestamptz, device int, temp float);
  SELECT create_hypertable('insert_dropchunks_race_t1', 'time', 'device', 2);
  INSERT INTO insert_dropchunks_race_t1 VALUES ('2020-01-03 10:30', 1, 32.2);
}

teardown {
  DROP TABLE insert_dropchunks_race_t1;
}

session "s1"
setup		{ BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1a"	{ INSERT INTO insert_dropchunks_race_t1 VALUES ('2020-01-03 10:30', 3, 33.4); }
step "s1b" 	{ COMMIT; }
step "s1c" 	{ SELECT COUNT(*) FROM _timescaledb_catalog.chunk_constraint LEFT JOIN _timescaledb_catalog.dimension_slice sl ON dimension_slice_id = sl.id WHERE sl.id IS NULL; }

session "s2"
setup	        { BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2a"	{ SELECT COUNT(*) FROM drop_chunks('insert_dropchunks_race_t1', TIMESTAMPTZ '2020-03-01'); }
step "s2b"	{ COMMIT; }

permutation "s1a" "s2a" "s1b" "s2b" "s1c"

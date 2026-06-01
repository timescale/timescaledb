# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# Race condition between insert and drop_chunks
#
# An insert that creates a new chunk inserts dimension_slice rows
# owned by that chunk. A concurrent drop_chunks cascades dimension_slice
# rows for the dropped chunks via FK ON DELETE CASCADE. This isolation
# test checks that the two paths do not leave any chunk lacking a
# dimension_slice row for any of its dimensions.

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
step "s1c" 	{
	SELECT COUNT(*)
	FROM _timescaledb_catalog.chunk c
	JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = c.hypertable_id
	LEFT JOIN _timescaledb_catalog.dimension_slice ds
	       ON ds.chunk_id = c.id AND ds.dimension_id = d.id
	WHERE NOT c.osm_chunk AND ds.id IS NULL;
}

session "s2"
setup	        { BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2a"	{ SELECT COUNT(*) FROM drop_chunks('insert_dropchunks_race_t1', TIMESTAMPTZ '2020-03-01'); }
step "s2b"	{ COMMIT; }

permutation "s1a" "s2a" "s1b" "s2b" "s1c"

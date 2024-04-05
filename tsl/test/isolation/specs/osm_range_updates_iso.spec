# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
  CREATE TABLE osm_test (time INTEGER, a INTEGER);
  SELECT create_hypertable('osm_test', 'time', chunk_time_interval => 10);
  CREATE TABLE osm_test2 (time INTEGER, a INTEGER);
  SELECT create_hypertable('osm_test2', 'time', chunk_time_interval => 10);
  INSERT INTO osm_test VALUES (1, 111);
  INSERT INTO osm_test2 VALUES (2, 211);
  UPDATE _timescaledb_catalog.hypertable set status = 3 WHERE table_name IN ('osm_test', 'osm_test2');
  UPDATE _timescaledb_catalog.chunk set osm_chunk = true WHERE hypertable_id IN (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name IN ('osm_test', 'osm_test2'));
  UPDATE _timescaledb_catalog.dimension_slice set range_start = 9223372036854775806, range_end = 9223372036854775807
  WHERE id IN (SELECT cc.dimension_slice_id FROM _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.chunk ch,
    _timescaledb_catalog.hypertable ht WHERE ht.id = ch.hypertable_id AND cc.chunk_id = ch.id AND ht.table_name IN ('osm_test', 'osm_test2'));

  CREATE TABLE test_drop(time INTEGER, a INTEGER);
  SELECT create_hypertable('test_drop', 'time', chunk_time_interval => 10);
  INSERT INTO test_drop(time, a) VALUES (1,1);
  UPDATE _timescaledb_catalog.hypertable set status = 1 WHERE table_name = 'test_drop';
  UPDATE _timescaledb_catalog.chunk set osm_chunk = true WHERE hypertable_id IN (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'test_drop');
  INSERT INTO test_drop VALUES (11, 11), (22, 22);
}

teardown {
  DROP TABLE osm_test;
  DROP TABLE osm_test2;
  DROP TABLE test_drop;
}


session "UR1"
step "UR1b" { BEGIN; }
step "UR1u" { SELECT _timescaledb_functions.hypertable_osm_range_update('osm_test', 0, 10); }
step "UR1c" { COMMIT; }

session "UR2"
step "UR2b" { BEGIN; }
step "UR2u" { SELECT _timescaledb_functions.hypertable_osm_range_update('osm_test', 0, 10); }
step "UR2c" { COMMIT; }

# lock dimension_slice tuple
session "LDST"
step "LockDimSliceTuple" {
  BEGIN;
  SELECT range_start, range_end FROM _timescaledb_catalog.dimension_slice
  WHERE id IN ( SELECT ds.id FROM 
    _timescaledb_catalog.chunk ch, _timescaledb_catalog.chunk_constraint cc,
    _timescaledb_catalog.dimension_slice ds, _timescaledb_catalog.hypertable ht
    WHERE ht.table_name like 'osm_test' AND cc.chunk_id = ch.id AND ht.id = ch.hypertable_id
    AND ds.id = cc.dimension_slice_id AND ch.osm_chunk = true
    ) FOR UPDATE;
  }
step "UnlockDimSliceTuple" { ROLLBACK; }

session "DT"
step "DTb" { BEGIN; }
step "DropOsmChunk" {
  SELECT _timescaledb_functions.drop_chunk(chunk_table::regclass)
  FROM (
    SELECT format('%I.%I', c.schema_name, c.table_name) as chunk_table
    FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
    WHERE ht.id = c.hypertable_id AND ht.table_name = 'osm_test'
  ) sq;
}
step "DTc" { COMMIT; }

session "LHT"
step "LHTb" { BEGIN; }
step "LockHypertableTuple" {
  SELECT table_name, compression_state, compressed_hypertable_id, status
  FROM _timescaledb_catalog.hypertable WHERE table_name = 'osm_test' FOR UPDATE;
}
step "UnlockHypertableTuple" { ROLLBACK; }

session "C"
step "Cb" { BEGIN; }
step "Cenable" {
  ALTER TABLE osm_test set (timescaledb.compress);
}
step "Ccommit" { COMMIT; }

session "AlterSchema"
step "Ab" { BEGIN; }
step "Aadd" { ALTER TABLE osm_test ADD COLUMN b INTEGER; }
step "Ac" { COMMIT; }

session "U2"
step "Utest2b" { BEGIN; }
step "Utest2u" { SELECT _timescaledb_functions.hypertable_osm_range_update('osm_test2', 0, 20); }
step "Utest2c" { COMMIT; }

session "DR1"
step DR1b { BEGIN; }
step DR1drop { 
  SELECT _timescaledb_functions.drop_osm_table_chunk('test_drop', chunk_table::regclass)
  FROM (
    SELECT format('%I.%I', c.schema_name, c.table_name) as chunk_table
    FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
    WHERE ht.id = c.hypertable_id AND ht.table_name = 'test_drop' AND c.osm_chunk IS TRUE
  ) sq;
}
step DR1c { COMMIT; }

session "DR2"
step DR2b { BEGIN; }
step DR2drop { 
  SELECT _timescaledb_functions.drop_osm_table_chunk('test_drop', chunk_table::regclass)
  FROM (
    SELECT format('%I.%I', c.schema_name, c.table_name) as chunk_table
    FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
    WHERE ht.id = c.hypertable_id AND ht.table_name = 'test_drop' AND c.osm_chunk IS TRUE
  ) sq;
}
step DR2c { COMMIT; }

# Concurrent updates will block one another
# this previously deadlocked one of the two transactions
permutation "LockDimSliceTuple" "UR1b" "UR1u" "UR2b" "UR2u" "UnlockDimSliceTuple" "UR1c" "UR2c"
# test concurrent delete of a chunk and range update. This should not be a valid scenario for
# an OSM chunk because it is maintained even if all its data is dropped/untiered, and its range
# is simply updated to reflect the fact it might be empty.
# However, it doesn't hurt to have a test for this, in case we decide to change this behavior in
# the future.
permutation "LockDimSliceTuple" "DTb" "UR1b" "DropOsmChunk" "UR1u" "UnlockDimSliceTuple" "DTc" "UR1c"
permutation "LockDimSliceTuple" "DTb" "UR1b" "UR1u" "DropOsmChunk" "UnlockDimSliceTuple" "UR1c" "DTc"
# one session enables compression -> thus changing the hypertable status
permutation "LHTb" "LockHypertableTuple" "Cb" "UR1b" "Cenable" "UR1u" "UnlockHypertableTuple" "Ccommit" "UR1c"

# schema changes to the table would propagate to the OSM chunk
# one session updates the range while the other updates the schema
permutation "Ab" "UR1b" "UR1u" "Aadd" "UR1c" "Ac"
permutation "Ab" "UR1b" "Aadd" "UR1u" "UR1c" "Ac"

# test with two hypertables both having osm chunks. Should not block one another. So once tuple of hypertable1 is unlocked, 
permutation "LHTb" "Utest2b" "UR1b" "LockHypertableTuple" "UR1u" "Utest2u" "Utest2c" "UnlockHypertableTuple" "UR1c"

# test two sessions concurrently dropping the OSM chunk
permutation "DR1b" "DR2b" "DR1drop" "DR2drop" "DR1c" "DR2c"

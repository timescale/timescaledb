# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
	CREATE TABLE osm_test (time INTEGER, a INTEGER);
  SELECT create_hypertable('osm_test', 'time', chunk_time_interval => 10);
  INSERT INTO osm_test VALUES (1, 111);
  UPDATE _timescaledb_catalog.hypertable set status = 3 WHERE table_name = 'osm_test';
  UPDATE _timescaledb_catalog.chunk set osm_chunk = true WHERE hypertable_id IN (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'osm_test');
  UPDATE _timescaledb_catalog.dimension_slice set range_start = 9223372036854775806, range_end = 9223372036854775807
    WHERE id IN (SELECT cc.dimension_slice_id FROM _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.chunk ch,
      _timescaledb_catalog.hypertable ht WHERE ht.id = ch.hypertable_id AND cc.chunk_id = ch.id AND ht.table_name = 'osm_test');
}

teardown {
	DROP TABLE osm_test;
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
  SELECT range_start, range_end from _timescaledb_catalog.dimension_slice
  WHERE id IN ( select ds.id FROM 
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
      SELECT '"' || c.schema_name || '"."' || c.table_name || '"' as chunk_table
      FROM _timescaledb_catalog.chunk c
    ) sq;
}
step "DTc" { COMMIT; }

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

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#check concurrent operations with freeze_chunk and unfreeze_chunk
setup {
  DROP TABLE IF EXISTS measurements;
  CREATE TABLE measurements (time timestamptz, device int , temp float);
  SELECT create_hypertable('measurements', 'time', chunk_time_interval=>'7 days'::interval);
  INSERT INTO measurements VALUES ('2020-01-03 10:30', 1, 1.0), ('2020-01-03 11:30', 2, 2.0);
  ALTER TABLE measurements SET (timescaledb.compress);

  CREATE OR REPLACE FUNCTION lock_chunktable( name text) RETURNS void AS $$
    BEGIN EXECUTE format( 'lock table %s IN SHARE MODE', name);
    END; $$ LANGUAGE plpgsql;

}

teardown {
  DROP TABLE measurements;
  DROP FUNCTION lock_chunktable;
}

# Test concurrent DML and freeze chunk. The wait point happens
# before a lock is acquired for freezing the chunk.

session "s1"
step "s1_begin_rr"  { 
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
}

step "s1_begin_s"  { 
	BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
}

step "s1_freeze"	{ SELECT _timescaledb_functions.freeze_chunk(ch) FROM (SELECT show_chunks('measurements') ch ) q; }
step "s1_unfreeze"	{ SELECT _timescaledb_functions.unfreeze_chunk(ch) FROM (SELECT show_chunks('measurements') ch ) q; }
step "s1_status" { SELECT ch.status  FROM _timescaledb_catalog.chunk ch 
                   WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable 
                   WHERE table_name = 'measurements'); }
step "s1_rollback" {
	ROLLBACK;
}

session "ins_S2"
step "s2_wp_enable"           { SELECT debug_waitpoint_enable('freeze_chunk_before_lock'); }
step "ins_s2_insert"  { BEGIN ; INSERT INTO measurements values('2020-01-03 12:30', 2, 2.0   ); }
step "ins_s2_commit"  { COMMIT; }
step "ins_s2_query"  { SELECT * FROM measurements ORDER BY 1; }
step "s2_wp_release"      { SELECT debug_waitpoint_release('freeze_chunk_before_lock'); }

session "upd_s2"
step "upd_s2_wp_enable"           { SELECT debug_waitpoint_enable('freeze_chunk_before_lock'); }
step "upd_s2_update"  { BEGIN ; UPDATE measurements SET temp = 200 WHERE device = 2; }
step "upd_s2_commit"  { COMMIT; }
step "upd_s2_query"  { SELECT * FROM measurements ORDER BY 1; }
step "upd_s2_wp_release"      { SELECT debug_waitpoint_release('freeze_chunk_before_lock'); }

session "del_s2"
step "del_s2_wp_enable"           { SELECT debug_waitpoint_enable('freeze_chunk_before_lock'); }
step "del_s2_delete"  { BEGIN ; DELETE FROM measurements WHERE device = 2; }
step "del_s2_commit"  { COMMIT; }
step "del_s2_query"  { SELECT * FROM measurements ORDER BY 1; }
step "del_s2_wp_release"      { SELECT debug_waitpoint_release('freeze_chunk_before_lock'); }

session "sel_S2"
step "sel_s2_wp_enable" {SELECT debug_waitpoint_enable('freeze_chunk_before_lock'); }
step "sel_s2_commit"  { COMMIT; }
step "sel_s2_query"  { BEGIN; SELECT * FROM measurements ORDER BY 1; }
step "sel_s2_wp_release"      { SELECT debug_waitpoint_release('freeze_chunk_before_lock'); }

session "LC"
step "BeginLock" {
	BEGIN;
}

step "LockChunk1" {
  SELECT
    lock_chunktable(format('%I.%I',ch.schema_name, ch.table_name))
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'measurements'
  ORDER BY ch.id LIMIT 1;
}

step "UpdateCatalogTupleStatus" {
  UPDATE _timescaledb_catalog.chunk SET status = 4 WHERE true;
}
step "Unlock" {ROLLBACK;}
step "CommitUpdate" {COMMIT;}

session "comp_s2"
step "comp_s2_compress" 
	{ BEGIN; SELECT CASE WHEN compress_chunk(ch) IS NOT NULL THEN 'Success' ELSE 'Failed' END  as COL FROM (SELECT show_chunks('measurements') ch ) q; }
step "comp_s2_commit" { COMMIT; }

session "decomp_s2"
step "decomp_s2_decompress" 
	{ BEGIN; SELECT CASE WHEN decompress_chunk(ch) IS NOT NULL THEN 'Success' ELSE 'Failed' END  as COL FROM (SELECT show_chunks('measurements') ch ) q; }
step "decomp_s2_commit" { COMMIT; }

#### freeze_chunk ####

###freeze_chunk waits for pending insert txns to complete, uncompressed and compressed variant
###transactions are serialized when they try to lock the chunk tuple entry in the catalog table. (see chunk.c)
permutation "s2_wp_enable" "ins_s2_insert" "s2_wp_release" "s1_freeze" "ins_s2_commit" "ins_s2_query" "s1_status"
permutation "comp_s2_compress" "comp_s2_commit" "s2_wp_enable" "ins_s2_insert" "s2_wp_release" "s1_freeze" "ins_s2_commit" "ins_s2_query" "s1_status"

###freeze_chunk waits for pending update txns to complete, uncompressed and compressed variant
###transactions are serialized when they try to lock the chunk tuple entry in the catalog table. (see chunk.c)
permutation "upd_s2_wp_enable" "upd_s2_update" "upd_s2_wp_release" "s1_freeze" "upd_s2_commit" "upd_s2_query" "s1_status"
permutation "comp_s2_compress" "comp_s2_commit" "upd_s2_wp_enable" "upd_s2_update" "upd_s2_wp_release" "s1_freeze" "upd_s2_commit" "upd_s2_query" "s1_status"

###freeze_chunk waits for pending delete txns to complete, uncompressed and compressed variant
###transactions are serialized when they try to lock the chunk tuple entry in the catalog table. (see chunk.c)
permutation "del_s2_wp_enable" "del_s2_delete" "del_s2_wp_release" "s1_freeze" "del_s2_commit" "del_s2_query" "s1_status"
permutation "comp_s2_compress" "comp_s2_commit" "del_s2_wp_enable" "del_s2_delete" "del_s2_wp_release" "s1_freeze" "del_s2_commit" "del_s2_query" "s1_status"

###freeze_chunk and select do not block each other, uncompressed and compressed variant
permutation "sel_s2_wp_enable" "sel_s2_query" "sel_s2_wp_release" "s1_freeze" "sel_s2_commit" "s1_status"
permutation "comp_s2_compress" "comp_s2_commit" "sel_s2_wp_enable" "sel_s2_query" "sel_s2_wp_release" "s1_freeze" "sel_s2_commit" "s1_status"

##if compress_chunk is in progress, freeze_chunk is blocked
permutation "BeginLock" "LockChunk1" "comp_s2_compress" "s1_freeze" "Unlock" "comp_s2_commit" "s1_status" 

##if decompress_chunk is in progress, freeze_chunk is blocked
permutation "comp_s2_compress" "comp_s2_commit" "BeginLock" "LockChunk1" "decomp_s2_decompress" "s1_freeze" "Unlock" "decomp_s2_commit" "s1_status" 

#### check higher isolation levels ####
permutation "BeginLock" "s1_begin_rr" "UpdateCatalogTupleStatus" "s1_freeze" "CommitUpdate" "s1_rollback"
permutation "BeginLock" "s1_begin_s" "UpdateCatalogTupleStatus" "s1_freeze" "CommitUpdate" "s1_rollback"

#### unfreeze_chunk ####

###unfreeze_chunk and select do not block each other, uncompressed and compressed variant
permutation "s1_freeze" "sel_s2_wp_enable" "sel_s2_query" "sel_s2_wp_release" "s1_unfreeze" "sel_s2_commit" "s1_status"
permutation "comp_s2_compress" "comp_s2_commit" "s1_freeze" "sel_s2_wp_enable" "sel_s2_query" "sel_s2_wp_release" "s1_unfreeze" "sel_s2_commit" "s1_status"

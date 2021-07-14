# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    CREATE TABLE ts_device_table(time INTEGER, device INTEGER, location INTEGER, value INTEGER);
    SELECT create_hypertable('ts_device_table', 'time', chunk_time_interval => 10);
    INSERT INTO ts_device_table SELECT generate_series(0,29,1), 1, 100, 20;
    ALTER TABLE ts_device_table set(timescaledb.compress, timescaledb.compress_segmentby='location', timescaledb.compress_orderby='time');
    CREATE FUNCTION lock_chunktable( name text) RETURNS void AS $$
    BEGIN EXECUTE format( 'lock table %s IN SHARE MODE', name);
    END; $$ LANGUAGE plpgsql;

}
teardown
{
   DROP TABLE ts_device_table cascade;
   DROP FUNCTION lock_chunktable( text );
}

session "I"
step "I1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 100, 100); }
step "Ic"   { COMMIT; }

session "IN"
step "IN1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 200, 100); }
step "INc"   { COMMIT; }

session "SI"
step "SChunkStat" {  SELECT status from _timescaledb_catalog.chunk 
       WHERE id = ( select min(ch.id) FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'); }

session "S"
step "S1" { SELECT count(*) from ts_device_table; }
step "SC1" { SELECT count(*) from _timescaledb_internal._hyper_1_1_chunk; }
step "SH" { SELECT total_chunks, number_compressed_chunks from hypertable_compression_stats('ts_device_table'); }

session "LCT"
step "LockChunkTuple" {
  BEGIN;
  SELECT status as chunk_status from _timescaledb_catalog.chunk 
  WHERE id = ( select min(ch.id) FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table') FOR UPDATE;
  }
step "UnlockChunkTuple"   { ROLLBACK; }
   
session "LC"
step "LockChunk1" {
  BEGIN;
  SELECT
    lock_chunktable(ch.schema_name || '.' || ch.table_name)
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'
  ORDER BY ch.id LIMIT 1;
}
step "UnlockChunk" {ROLLBACK;}

session "C"
step "C1"   {
  BEGIN;
  SET LOCAL lock_timeout = '500ms';
  SET LOCAL deadlock_timeout = '10ms';
  SELECT
    compress_chunk(ch.schema_name || '.' || ch.table_name)
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'
  ORDER BY ch.id LIMIT 1;
}
step "Cc"   { COMMIT; }

session "A"
step "A1" { BEGIN; ALTER TABLE ts_device_table SET ( fillfactor = 80); }
step "A2" { COMMIT; }

session "D"
step "D1"   {
  BEGIN;
  SELECT
    decompress_chunk(ch.schema_name || '.' || ch.table_name)
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'
  ORDER BY ch.id LIMIT 1;
}
step "Dc"   { COMMIT; }

session "CompressAll"
step "CA1" {
  BEGIN;
  SELECT
    compress_chunk(ch.schema_name || '.' || ch.table_name)
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'
  ORDER BY ch.id;
}
step "CAc" { COMMIT; }

session "RecompressChunk"
step "RC1" {
  BEGIN;
  SELECT
    recompress_chunk(ch.schema_name || '.' || ch.table_name)
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'
  ORDER BY ch.id LIMIT 1;

}
step "RCc" { COMMIT; }
#if insert in progress, compress  is blocked
permutation "LockChunk1" "I1" "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" 

#Compress in progress, insert is blocked
permutation "LockChunk1" "C1" "I1" "UnlockChunk" "Cc" "Ic"  

#if ddl in progress, compress_chunk blocked
permutation "LockChunk1" "A1" "C1" "UnlockChunk" "Cc" "A2"
permutation "LockChunk1" "A1" "C1" "UnlockChunk" "A2" "Cc"

#concurrent compress/deocmpress on same chunk errors
permutation "LockChunk1" "C1" "D1" "UnlockChunk" "Cc" "Dc"

#concurrent compress and select should execute concurrently
permutation "LockChunk1" "C1" "S1" "UnlockChunk" "Cc" "SH" 
permutation "LockChunk1" "C1" "S1" "UnlockChunk" "SH" "Cc"

#concurrent inserts into compressed chunk will wait to update chunk status
# and not error out.
permutation "C1" "Cc" "LockChunkTuple" "I1" "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat"

#concurrent recompress and insert. insert will succeed after recompress
# completes
#first compress chunk and insert into chunk, then start concurrent processes
#both recompress_chunk and insert wait for lock on the chunk 
permutation "CA1" "CAc" "I1" "Ic" "SChunkStat" "LockChunk1" "RC1" "IN1"  "UnlockChunk" "RCc" "INc" "SH"

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    CREATE TABLE ts_device_table(time INTEGER, device INTEGER, location INTEGER, value INTEGER);
    SELECT create_hypertable('ts_device_table', 'time', chunk_time_interval => 10);
    INSERT INTO ts_device_table SELECT generate_series(0,29,1), 1, 100, 20;
    ALTER TABLE ts_device_table set(timescaledb.compress, timescaledb.compress_segmentby='location', timescaledb.compress_orderby='time');
    CREATE OR REPLACE FUNCTION lock_chunktable( name text) RETURNS void AS $$
    BEGIN EXECUTE format( 'lock table %s IN SHARE MODE', name);
    END; $$ LANGUAGE plpgsql;
    CREATE FUNCTION count_chunktable(tbl regclass) RETURNS TABLE("count(*)" int, "count(*) only" int) AS $$
    DECLARE c int;c_only int;
    BEGIN
      EXECUTE format('SELECT count(*) FROM %s', tbl) INTO c;
      EXECUTE format('SELECT count(*) FROM ONLY %s', tbl) INTO c_only;
      RETURN QUERY SELECT c,c_only;
    END; $$ LANGUAGE plpgsql;
}
teardown
{
   DROP TABLE ts_device_table cascade;
   DROP FUNCTION lock_chunktable;
   DROP FUNCTION count_chunktable;
}

session "I"
step "I1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 100, 100); }
step "IR1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 100, 100) RETURNING *; }
step "Ic"   { COMMIT; }

session "IN"
step "IN1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 200, 100); }
step "INR1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 200, 100) RETURNING *; }
step "INc"   { COMMIT; }

session "SI"
step "SChunkStat" {  SELECT status from _timescaledb_catalog.chunk
       WHERE id = ( select min(ch.id) FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'); }

session "S"
step "S1" { SELECT count(*) from ts_device_table; }
step "SC1" { SELECT (count_chunktable(ch)).* FROM show_chunks('ts_device_table') AS ch ORDER BY ch::text LIMIT 1; }
step "SH" { SELECT total_chunks, number_compressed_chunks from hypertable_compression_stats('ts_device_table'); }
step "SA" { SELECT * FROM ts_device_table; }


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
    lock_chunktable(format('%I.%I',ch.schema_name, ch.table_name))
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
    compress_chunk(format('%I.%I',ch.schema_name, ch.table_name)) IS NOT NULL AS compress
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
  SET LOCAL client_min_messages TO WARNING;
  SELECT
    decompress_chunk(ch) IS NOT NULL AS decompress
  FROM show_chunks('ts_device_table') AS ch
  ORDER BY ch::text LIMIT 1;
}
step "Dc"   { COMMIT; }

session "CompressAll"
step "CA1" {
  BEGIN;
  SELECT
    compress_chunk(ch) IS NOT NULL AS compress
  FROM show_chunks('ts_device_table') AS ch
  ORDER BY ch::text;
}
step "CAc" { COMMIT; }

session "RecompressChunk1"
step "RC1" {
  DO $$
  DECLARE
    chunk_name text;
  BEGIN
  FOR chunk_name IN
      SELECT ch FROM show_chunks('ts_device_table') ch
      LIMIT 1
     LOOP
         PERFORM compress_chunk(chunk_name);
     END LOOP;
  END;
  $$;
}

session "RecompressChunk2"
step "RC2" {
  DO $$
  DECLARE
    chunk_name text;
  BEGIN
  FOR chunk_name IN
      SELECT ch FROM show_chunks('ts_device_table') ch
       ORDER BY ch::text LIMIT 1
     LOOP
         PERFORM compress_chunk(chunk_name, false);
     END LOOP;
  END;
  $$;
}


#if insert in progress, compress  is blocked
permutation "LockChunk1" "I1" "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1"

#Compress in progress, insert is blocked
permutation "LockChunk1" "C1" "I1" "UnlockChunk" "Cc" "Ic"

#if ddl in progress, compress_chunk blocked
permutation "LockChunk1" "A1" "C1" "UnlockChunk" "Cc" "A2"
permutation "LockChunk1" "A1" "C1" "UnlockChunk" "A2" "Cc"

#concurrent compress/decompress on same chunk errors
permutation "LockChunk1" "C1" "D1" "UnlockChunk" "Cc" "Dc"

#concurrent compress and select should execute concurrently
permutation "LockChunk1" "C1" "S1" "UnlockChunk" "Cc" "SH"
permutation "LockChunk1" "C1" "S1" "UnlockChunk" "SH" "Cc"

# concurrent inserts into compressed chunk will wait to update chunk status
# and not error out.
permutation "C1" "Cc" "LockChunkTuple" "I1" "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat"

#concurrent inserts don't need to wait for each other if inserting into a partially compressed chunk
#commit order should not matter in that case
permutation "C1" "Cc" "I1" "Ic" "LockChunkTuple" "I1" "IN1"  "UnlockChunkTuple" "INc" "Ic" "SChunkStat"

#concurrent inserts into compressed chunk with RETURNING clause should behave the same
permutation "C1" "Cc" "LockChunkTuple" "IR1" "INR1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat"
permutation "C1" "Cc" "I1" "Ic" "LockChunkTuple" "IR1" "INR1"  "UnlockChunkTuple" "INc" "Ic" "SChunkStat"

# Testing concurrent recompress and insert.

# Insert will succeed after first phase of recompress completes.

# - First compress chunk and insert into chunk
# - Then start concurrent processes both recompress_chunk and insert
# - Wait for lock on the chunk.
permutation "CA1" "CAc" "I1" "Ic" "SChunkStat" "LockChunk1" "RC1" "IN1"  "UnlockChunk" "INc" "SH" "SA" "SChunkStat"

# Test concurrent recompress operations
permutation "CA1" "CAc" "I1" "Ic" "SChunkStat" "LockChunk1" "RC1" "RC2"  "UnlockChunk" "SH" "SA" "SChunkStat"

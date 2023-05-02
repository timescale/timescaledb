# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    CREATE TABLE ts_device_table(time INTEGER, device INTEGER, location INTEGER, value INTEGER);
    CREATE UNIQUE INDEX device_time_idx on ts_device_table(time, device);
    SELECT create_hypertable('ts_device_table', 'time', chunk_time_interval => 10);
    INSERT INTO ts_device_table SELECT generate_series(0,9,1), 1, 100, 20;
    ALTER TABLE ts_device_table set(timescaledb.compress, timescaledb.compress_segmentby='location', timescaledb.compress_orderby='time');
    CREATE FUNCTION lock_chunktable( name text) RETURNS void AS $$
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
step "IB" { BEGIN; }
step "IBRR" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "IBS" { BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "I1"   { 
    INSERT INTO ts_device_table VALUES (1, 1, 100, 100) ON CONFLICT DO NOTHING; 
}
step "Iu1"   { 
    INSERT INTO ts_device_table VALUES (1, 1, 100, 98) ON CONFLICT(time, device) DO UPDATE SET value = 98; 
}
step "Ic"   { COMMIT; }

session "IN"
step "IN1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 200, 100) ON CONFLICT DO NOTHING; }
step "INu1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 100, 99) ON CONFLICT(time, device) DO UPDATE SET value = 99; }
step "INc"   { COMMIT; }

session "SI"
step "SChunkStat" {  SELECT status from _timescaledb_catalog.chunk
       WHERE id = ( select min(ch.id) FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'); }

session "S"
step "S1" { SELECT count(*) from ts_device_table; }
step "SC1" { SELECT (count_chunktable(ch)).* FROM show_chunks('ts_device_table') AS ch LIMIT 1; }
step "SH" { SELECT total_chunks, number_compressed_chunks from hypertable_compression_stats('ts_device_table'); }
step "SA" { SELECT * FROM ts_device_table; }
step "SU" { SELECT * FROM ts_device_table WHERE value IN (98,99); }


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
    CASE WHEN compress_chunk(format('%I.%I',ch.schema_name, ch.table_name)) IS NOT NULL THEN true ELSE false END AS compress
  FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch
  WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'
  ORDER BY ch.id LIMIT 1;
}
step "Cc"   { COMMIT; }

session "CompressAll"
step "CA1" {
  BEGIN;
  SELECT
    CASE WHEN compress_chunk(ch) IS NOT NULL THEN true ELSE false END AS compress
  FROM show_chunks('ts_device_table') AS ch
  ORDER BY ch::text;
}
step "CAc" { COMMIT; }

session "RecompressChunk"
step "RC" {
  DO $$
  DECLARE
    chunk_name text;
  BEGIN
  FOR chunk_name IN
      SELECT ch FROM show_chunks('ts_device_table') ch
       ORDER BY ch::text LIMIT 1
     LOOP
         CALL recompress_chunk(chunk_name);
     END LOOP;
  END;
  $$;
}

#If insert is in progress, compression  is blocked.
permutation "LockChunk1" "IB"   "I1"   "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" "SChunkStat"
permutation "LockChunk1" "IBRR" "I1"   "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" "SChunkStat"
permutation "LockChunk1" "IBS"  "I1"   "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" "SChunkStat"
permutation "LockChunk1" "IB"   "Iu1"  "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" "SU" "SChunkStat"
permutation "LockChunk1" "IBRR" "Iu1"  "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" "SU" "SChunkStat"
permutation "LockChunk1" "IBS"  "Iu1"  "C1" "UnlockChunk" "Ic" "Cc" "SC1" "S1" "SU" "SChunkStat"

#Compress in progress, insert is blocked
permutation "LockChunk1" "C1" "IB"   "I1"  "UnlockChunk" "Cc" "Ic" "SC1" "SA" "SChunkStat"
permutation "LockChunk1" "C1" "IBRR" "I1"  "UnlockChunk" "Cc" "Ic" "SC1" "SA" "SChunkStat"
permutation "LockChunk1" "C1" "IBS"  "I1"  "UnlockChunk" "Cc" "Ic" "SC1" "SA" "SChunkStat"
permutation "LockChunk1" "C1" "IB"   "Iu1" "UnlockChunk" "Cc" "Ic" "SC1" "SA" "SChunkStat"
permutation "LockChunk1" "C1" "IBRR" "Iu1" "UnlockChunk" "Cc" "Ic" "SC1" "SA" "SChunkStat"
permutation "LockChunk1" "C1" "IBS"  "Iu1" "UnlockChunk" "Cc" "Ic" "SC1" "SA" "SChunkStat"

# Concurrent inserts into compressed chunk will update chunk status and not error out.
permutation "C1" "Cc" "LockChunkTuple" "IB"   "I1"  "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBRR" "I1"  "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBS"  "I1"  "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IB"   "I1"  "INu1" "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SU" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBRR" "I1"  "INu1" "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBS"  "I1"  "INu1" "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IB"   "Iu1" "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SU" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBRR" "Iu1" "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBS"  "Iu1" "IN1"  "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IB"   "Iu1" "INu1" "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SU" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBRR" "Iu1" "INu1" "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"
permutation "C1" "Cc" "LockChunkTuple" "IBS"  "Iu1" "INu1" "UnlockChunkTuple" "Ic" "INc" "SChunkStat" "SA"

# Testing concurrent recompress and insert.

# Insert will succeed after first phase of recompress completes.

# - First compress chunk and insert into chunk
# - Then start concurrent processes both recompress_chunk and insert
# - Wait for lock on the chunk.
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IB"   "I1"   "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IBRR" "I1"   "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IBS"  "I1"   "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IB"   "Iu1"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IBRR" "Iu1"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat" "SU"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IBS"  "Iu1"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat" "SU"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "IN1"  "UnlockChunk" "INc" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "RC" "INu1" "UnlockChunk" "INc" "SH" "SA" "SChunkStat" "SU"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IB"   "I1"  "RC"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IBRR" "I1"  "RC"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IBS"  "I1"  "RC"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IB"   "Iu1" "RC"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat" "SU"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IBRR" "Iu1" "RC"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat" "SU"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IBS"  "Iu1" "RC"  "UnlockChunk" "Ic" "SH" "SA" "SChunkStat" "SU"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "IN1"  "RC" "UnlockChunk" "INc" "SH" "SA" "SChunkStat"
permutation "CA1" "CAc" "I1" "SChunkStat" "LockChunk1" "INu1" "RC" "UnlockChunk" "INc" "SH" "SA" "SChunkStat" "SU"

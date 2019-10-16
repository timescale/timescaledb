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
step "Ib"   { BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms';}
step "I1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 100, 100); }
step "Ic"   { COMMIT; }

session "S"
step "St" {BEGIN;} 
step "S1" { SELECT count(*) from ts_device_table; }
step "SC1" { SELECT count(*) from _timescaledb_internal._hyper_1_1_chunk; }
step "SH" { SELECT total_chunks, number_compressed_chunks from timescaledb_information.compressed_hypertable_stats where hypertable_name::text like 'ts_device_table'; }
step "Sc" {COMMIT;}

session "LC"
step "LockChunk1"   { BEGIN; select lock_chunktable (q.chname) from (select ch.schema_name || '.' || ch.table_name as chname from _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch where ch.hypertable_id = ht.id and ht.table_name like 'ts_device_table' order by ch.id limit 1 ) q; }
step "UnlockChunk" {ROLLBACK;}

session "C"
step "C1"   { BEGIN; SELECT compress_chunk(q.chname) from (select ch.schema_name || '.' || ch.table_name as chname from _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch where ch.hypertable_id = ht.id and ht.table_name like 'ts_device_table' order by ch.id limit 1 ) q; }
step "Cc"   { COMMIT; }

session "A"
step "A1" { BEGIN; ALTER TABLE ts_device_table SET ( fillfactor = 80); }
step "A2" { COMMIT; }

session "D"
step "D1"   { BEGIN; SELECT decompress_chunk(q.chname) from (select ch.schema_name || '.' || ch.table_name as chname from _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch where ch.hypertable_id = ht.id and ht.table_name like 'ts_device_table' order by ch.id limit 1 ) q; }
step "Dc"   { COMMIT; }

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

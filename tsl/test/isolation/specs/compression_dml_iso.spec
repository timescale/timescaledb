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
step "Ic"   { COMMIT; }

session "IN"
step "IN1"   { BEGIN; INSERT INTO ts_device_table VALUES (1, 1, 200, 100); }
step "INc"   { COMMIT; }

session "DEL"
step "DEL1"   { BEGIN; DELETE from ts_device_table WHERE location = 200; }
step "DELc"   { COMMIT; }

session "UPD"
step "UPD1"   { BEGIN; UPDATE ts_device_table SET value = 4 WHERE location = 200; }
step "UPDc"   { COMMIT; }

session "SI"
step "SChunkStat" {  SELECT status from _timescaledb_catalog.chunk
       WHERE id = ( select min(ch.id) FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.chunk ch WHERE ch.hypertable_id = ht.id AND ht.table_name like 'ts_device_table'); }

session "S"
step "S1" { SELECT count(*) from ts_device_table; }
step "SC1" { SELECT (count_chunktable(ch)).* FROM show_chunks('ts_device_table') AS ch ORDER BY ch::text LIMIT 1; }
step "SH" { SELECT total_chunks, number_compressed_chunks from hypertable_compression_stats('ts_device_table'); }
step "SA" { SELECT * FROM ts_device_table; }

session "CompressAll"
step "CA1" {
  BEGIN;
  SELECT
    CASE WHEN compress_chunk(ch) IS NOT NULL THEN true ELSE false END AS compress
  FROM show_chunks('ts_device_table') AS ch
  ORDER BY ch::text;
}
step "CAc" { COMMIT; }

# Test concurrent update/delete operations
permutation "CA1" "CAc" "SH" "I1" "Ic" "SH" "UPD1" "UPDc" "SH" "DEL1" "DELc" "SH" "UPD1" "UPDc" "SH"
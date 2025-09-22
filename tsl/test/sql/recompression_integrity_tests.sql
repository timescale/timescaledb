-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE IF NOT EXISTS data_integrity_snapshots (
    table_name text,
    snapshot_data jsonb
);

CREATE OR REPLACE FUNCTION capture_data_snapshot(p_chunk regclass)
RETURNS void AS $$
DECLARE
    sql text;
BEGIN
    DELETE FROM data_integrity_snapshots
    WHERE table_name = p_chunk::text;

    sql := format($f$
        INSERT INTO data_integrity_snapshots (table_name, snapshot_data)
        SELECT %L, jsonb_agg(to_jsonb(t.*) ORDER BY to_jsonb(t.*))
        FROM %s t
    $f$, p_chunk::text, p_chunk);

    EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_data_integrity(p_chunk regclass)
RETURNS boolean AS $$
DECLARE
    current_data   jsonb := '[]'::jsonb;
    original_data  jsonb;
    current_count  bigint;
    original_count bigint;
    sql            text;
BEGIN
    -- Build query with regclass so schema/table are quoted correctly
    sql := format($f$
        SELECT COALESCE(jsonb_agg(to_jsonb(t.*) ORDER BY to_jsonb(t.*)), '[]'::jsonb)
        FROM %s t
    $f$, p_chunk);

    EXECUTE sql INTO current_data;

    -- Load snapshot for this table
    SELECT snapshot_data
      INTO original_data
      FROM data_integrity_snapshots
     WHERE table_name = p_chunk::text;

    IF original_data IS NULL THEN
        RAISE EXCEPTION 'No original data snapshot found for table %', p_chunk::text;
        RETURN false;
    END IF;

    current_count  := jsonb_array_length(current_data);
    original_count := jsonb_array_length(original_data);

    IF current_count <> original_count THEN
        RAISE EXCEPTION 'Row count mismatch for %: expected %, got %', p_chunk::text, original_count, current_count;
        RETURN false;
    END IF;

    -- Compare content
    IF current_data IS DISTINCT FROM original_data THEN
        RAISE EXCEPTION 'Data content mismatch for table %', p_chunk::text;
        RETURN false;
    END IF;

    RAISE NOTICE 'Data integrity validation passed for % (% rows)', p_chunk::text, current_count;
    RETURN true;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_recompression(p_table_name text)
RETURNS boolean AS $$
DECLARE
    old_chunk_name text;
    old_compressed_chunk_name text;
    old_chunk_id int;
    new_compressed_chunk_name text;
    new_chunk_id int;
    ok boolean := false;
BEGIN
    -- Compress all uncompressed chunks
    PERFORM compress_chunk(ch)
    FROM show_chunks(p_table_name) ch;

    -- Store initial compressed chunk info before recompression
    SELECT uncompressed.schema_name || '.' || uncompressed.table_name,
           compressed.schema_name || '.' || compressed.table_name,
           compressed.id
      INTO old_chunk_name, old_compressed_chunk_name, old_chunk_id
    FROM _timescaledb_catalog.chunk uncompressed
    JOIN _timescaledb_catalog.chunk compressed
      ON uncompressed.compressed_chunk_id = compressed.id
    WHERE uncompressed.hypertable_id = (
              SELECT id
              FROM _timescaledb_catalog.hypertable
              WHERE table_name = p_table_name
          )
    LIMIT 1;

    IF old_chunk_name IS NULL THEN
        RAISE EXCEPTION 'No chunk found for hypertable %', p_table_name;
        RETURN false;
    END IF;

    -- Capture original data snapshot
    PERFORM capture_data_snapshot(old_chunk_name);

    -- Recompress the chunk in-memory
    PERFORM compress_chunk(old_chunk_name, recompress := true);

    -- Get info for the new compressed chunk
    SELECT compressed.schema_name || '.' || compressed.table_name,
           compressed.id
      INTO new_compressed_chunk_name, new_chunk_id
    FROM _timescaledb_catalog.chunk uncompressed
    JOIN _timescaledb_catalog.chunk compressed
      ON uncompressed.compressed_chunk_id = compressed.id
    WHERE uncompressed.schema_name || '.' || uncompressed.table_name = old_chunk_name
    LIMIT 1;

    -- Check if a new chunk was created
    IF new_chunk_id IS NULL OR old_chunk_id = new_chunk_id THEN
        RAISE EXCEPTION 'Recompression did not create a new chunk for %', p_table_name;
        RETURN false;
    END IF;

    -- Validate integrity of recompressed data
    ok := validate_data_integrity(old_chunk_name);

    IF ok THEN
        RAISE NOTICE 'Recompression test passed for %, old_chunk_id=%, new_chunk_id=%',
                     p_table_name, old_chunk_id, new_chunk_id;
    ELSE
        RAISE EXCEPTION 'Data integrity validation failed for %', p_table_name;
    END IF;

    RETURN ok;
END;
$$ LANGUAGE plpgsql;

-- Test Case 1: Basic segmentby configuration
DROP TABLE IF EXISTS recomp_segmentby_test CASCADE;
CREATE TABLE recomp_segmentby_test(
    time timestamptz NOT NULL,
    device text,
    location text,
    temperature float,
    humidity float
);
SELECT table_name FROM create_hypertable('recomp_segmentby_test','time') \gset

-- Set compression with single segmentby column
ALTER TABLE recomp_segmentby_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby='device',
    timescaledb.compress_orderby='time'
);

-- Insert test data across multiple time ranges
INSERT INTO recomp_segmentby_test VALUES
  ('2000-01-01 00:00:00', 'device1', 'room1', 20.5, 60.0),
  ('2000-01-01 01:00:00', 'device1', 'room1', 21.0, 61.5),
  ('2000-01-01 02:00:00', 'device2', 'room2', 19.8, 58.2),
  ('2000-01-02 00:00:00', 'device1', 'room1', 22.1, 63.0),
  ('2000-01-02 01:00:00', 'device2', 'room2', 18.9, 55.7),
  ('2001-01-01 00:00:00', 'device1', 'room1', 25.0, 70.0),
  ('2001-01-01 01:00:00', 'device2', 'room2', 23.5, 68.5);

SELECT test_recompression(:'table_name');

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_segmentby_test CASCADE;

-- Test Case 2: Multiple segmentby columns configuration
DROP TABLE IF EXISTS recomp_multi_segmentby_test CASCADE;
CREATE TABLE recomp_multi_segmentby_test(
    time timestamptz NOT NULL,
    device text,
    location text,
    sensor_type text,
    value float
);
SELECT table_name FROM create_hypertable('recomp_multi_segmentby_test','time') \gset

-- Set compression with multiple segmentby columns
ALTER TABLE recomp_multi_segmentby_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby='device,location',
    timescaledb.compress_orderby='time'
);

-- Insert test data
INSERT INTO recomp_multi_segmentby_test VALUES
  ('2000-01-01', 'device1', 'room1', 'temp', 20.5),
  ('2000-01-01', 'device1', 'room1', 'humidity', 60.0),
  ('2000-01-01', 'device1', 'room2', 'temp', 21.0),
  ('2000-01-01', 'device2', 'room1', 'temp', 19.8),
  ('2000-01-01', 'device2', 'room2', 'humidity', 58.2),
  ('2001-01-01', 'device1', 'room1', 'temp', 22.1),
  ('2001-01-01', 'device2', 'room1', 'humidity', 63.0);

SELECT test_recompression(:'table_name');

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_multi_segmentby_test CASCADE;

-- Test Case 3: Sparse index configuration
DROP TABLE IF EXISTS recomp_index_test CASCADE;
CREATE TABLE recomp_index_test(
    x int,
    value text,
    u uuid,
    ts timestamp
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='x',
    tsdb.segment_by='',
    tsdb.order_by='x',
    tsdb.index='bloom("u"),minmax("ts")'
);

-- Insert test data with sparse UUID pattern
INSERT INTO recomp_index_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2021-01-01'::timestamp + (interval '1 hour') * x
FROM generate_series(1, 10000) x;

INSERT INTO recomp_index_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2022-01-01'::timestamp + (interval '1 hour') * x
FROM generate_series(1, 10000) x;

SELECT test_recompression('recomp_index_test');

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_index_test CASCADE;

-- Test Case 4: Large dataset
DROP TABLE IF EXISTS recomp_large_data_test CASCADE;
CREATE TABLE recomp_large_data_test(
    x int,
    value text,
    u uuid,
    ts timestamp
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='ts',
    tsdb.segment_by='',
    tsdb.order_by='ts',
    tsdb.index=''
);

-- Insert test data with sparse UUID pattern
INSERT INTO recomp_large_data_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2021-01-01'::timestamp + (interval '1 hour') * x
FROM generate_series(1, 10000) x;

INSERT INTO recomp_large_data_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2022-01-01'::timestamp + (interval '1 hour') * x
FROM generate_series(1, 10000) x;

SELECT test_recompression('recomp_large_data_test');

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_large_data_test CASCADE;

-- Cleanup
DROP FUNCTION IF EXISTS validate_data_integrity(regclass);
DROP FUNCTION IF EXISTS capture_data_snapshot(regclass);
DROP TABLE IF EXISTS data_integrity_snapshots CASCADE;





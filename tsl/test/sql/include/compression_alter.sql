-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test disabling compression on hypertables with caggs and dropped chunks
-- github issue 2844
CREATE TABLE i2844 (created_at timestamptz NOT NULL,c1 float);
SELECT create_hypertable('i2844', 'created_at', chunk_time_interval => '6 hour'::interval);
INSERT INTO i2844 SELECT generate_series('2000-01-01'::timestamptz, '2000-01-02'::timestamptz,'1h'::interval);

CREATE MATERIALIZED VIEW test_agg WITH (timescaledb.continuous) AS SELECT time_bucket('1 hour', created_at) AS bucket, AVG(c1) AS avg_c1 FROM i2844 GROUP BY bucket;

ALTER TABLE i2844 SET (timescaledb.compress);

SELECT compress_chunk(show_chunks) AS compressed_chunk FROM show_chunks('i2844');
SELECT drop_chunks('i2844', older_than => '2000-01-01 18:00'::timestamptz);
SELECT decompress_chunk(show_chunks, if_compressed => TRUE) AS decompressed_chunks FROM show_chunks('i2844');

ALTER TABLE i2844 SET (timescaledb.compress = FALSE);

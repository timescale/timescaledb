# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test concurrent drop_chunks and compress_chunk
#

# Create three chunks
setup
{
	CREATE TABLE conditions (time timestamptz, temp float);
	SELECT create_hypertable('conditions', 'time', chunk_time_interval => interval '1 day');
	INSERT INTO conditions
	SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-03 00:00','1 hour'), random() * 100;
	ALTER TABLE conditions SET (timescaledb.compress = true);
}

teardown {
	DROP TABLE conditions;
}

session "s1"
setup	{
	BEGIN;
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET LOCAL lock_timeout = '500ms';
	SET LOCAL deadlock_timeout = '300ms';
}

# Drop two chunks
step "s1_drop"	 {
	SELECT count (*)
	FROM drop_chunks('conditions', older_than => '2018-12-03 00:00'::timestamptz);
}
step "s1_commit" { COMMIT; }

session "s2"
setup	{
	BEGIN;
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET LOCAL lock_timeout = '500ms';
	SET LOCAL deadlock_timeout = '300ms';

	CREATE TEMPORARY TABLE IF NOT EXISTS chunks_to_compress ON COMMIT DROP AS
	SELECT chunk
	FROM show_chunks('conditions', older_than => '2018-12-03 00:00'::timestamptz) chunk
	ORDER BY 1 LIMIT 2;
}

# Compress same two chunks as are being dropped
step "s2_compress_chunk_1" {
	SELECT count(compress_chunk(chunk))
	FROM (SELECT chunk FROM chunks_to_compress ORDER BY 1 ASC LIMIT 1) AS chunk;
}

step "s2_compress_chunk_2" {
	SELECT count(compress_chunk(chunk))
	FROM (SELECT chunk FROM chunks_to_compress ORDER BY 1 DESC LIMIT 1) AS chunk;
}

step "s2_commit"   { COMMIT; }

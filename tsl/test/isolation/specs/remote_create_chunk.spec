# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test concurrent remote chunk creation
#

setup
{
	CREATE TABLE conditions (time timestamptz, device int, temp float);
	SELECT create_hypertable('conditions', 'time', 'device', 2, chunk_time_interval => interval '1 day');

	CREATE OR REPLACE FUNCTION waitpoint_count(tag TEXT) RETURNS bigint AS
	$$
		SELECT count(*)
		FROM pg_locks
		WHERE locktype = 'advisory'
		AND mode = 'ShareLock'
		AND objid = debug_waitpoint_id(tag)
		AND granted = false;
	$$ LANGUAGE SQL;

	CREATE OR REPLACE FUNCTION root_table_lock_count() RETURNS bigint AS
	$$
		SELECT count(*) AS num_locks_on_conditions_table
		FROM pg_locks
		WHERE locktype = 'relation'
		AND relation = 'conditions'::regclass
		AND mode = 'ShareUpdateExclusiveLock';
	$$ LANGUAGE SQL;
}

teardown {
	DROP TABLE conditions;
}


session "s1"
setup	{
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET LOCAL lock_timeout = '500ms';
	SET LOCAL deadlock_timeout = '300ms';
	SET application_name = 's1';
}

# Try to create a chunk
step "s1_create_chunk_1" {
	SELECT slices, created FROM _timescaledb_functions.create_chunk('conditions', jsonb_build_object('time', ARRAY[1514764800000000, 1514851200000000], 'device', ARRAY[-9223372036854775808, 1073741823]));
}

# Create a chunk that does not exist
session "s2"
setup	{
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET LOCAL lock_timeout = '500ms';
	SET LOCAL deadlock_timeout = '300ms';
	SET application_name = 's1';
}

step "s2_create_chunk_1" {
	SELECT slices, created FROM _timescaledb_functions.create_chunk('conditions', jsonb_build_object('time', ARRAY[1514764800000000, 1514851200000000], 'device', ARRAY[-9223372036854775808, 1073741823]));
}

step "s2_create_chunk_2" {
	SELECT slices, created FROM _timescaledb_functions.create_chunk('conditions', jsonb_build_object('time', ARRAY[1514764800000000, 1514851200000000], 'device', ARRAY[1073741823, 9223372036854775807]));
}

session "s3"
setup {
	SET application_name = 's3';
}

step "s3_conditions_locks" {
	SELECT root_table_lock_count() AS table_lock,
	waitpoint_count('find_or_create_chunk_start') AS at_start,
	waitpoint_count('find_or_create_chunk_created') AS at_created,
	waitpoint_count('find_or_create_chunk_found') AS at_found;
}

session "s4"
step "s4_wait_start"           { SELECT debug_waitpoint_enable('find_or_create_chunk_start'); }
step "s4_wait_created"           { SELECT debug_waitpoint_enable('find_or_create_chunk_created'); }
step "s4_wait_found"           { SELECT debug_waitpoint_enable('find_or_create_chunk_found'); }
step "s4_release_start"      { SELECT debug_waitpoint_release('find_or_create_chunk_start'); }
step "s4_release_created"      { SELECT debug_waitpoint_release('find_or_create_chunk_created'); }
step "s4_release_found"      { SELECT debug_waitpoint_release('find_or_create_chunk_found'); }

# s1 and s2 will try create the same chunk and both take the lock on
# the root table. However, s2 will find the chunk created by s1 after
# s1 releases the root lock and then s2 will also release the root
# lock before transaction end.
permutation "s4_wait_created" "s4_wait_found" "s1_create_chunk_1" "s2_create_chunk_1" "s3_conditions_locks" "s4_release_created" "s3_conditions_locks" "s4_release_found"

# s1 and s2 will create different chunks and both will try to lock the
# root table. They will each create their own unique chunks, so s2
# won't block on the "found" wait point.
permutation "s4_wait_created" "s4_wait_found" "s1_create_chunk_1" "s2_create_chunk_2" "s3_conditions_locks" "s4_release_created" "s3_conditions_locks" "s4_release_found"

# s1 and s2 runs concurrently and both try to create the same
# chunk. However, s1 completes its transaction before s2 looks up the
# chunk. Therefore s2 will find the new chunk and need not take the
# lock on the root table.
permutation "s4_wait_created" "s1_create_chunk_1" "s4_wait_start" "s2_create_chunk_1" "s3_conditions_locks" "s4_release_created" "s3_conditions_locks" "s4_release_start" "s3_conditions_locks"

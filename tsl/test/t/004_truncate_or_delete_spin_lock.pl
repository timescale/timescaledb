# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# This TAP test verifies the spin lock behaviour when the `timescaledb.compress_truncate_behaviour` GUC is set to `truncate_or_delete`
# The order of operations tested are the same as the `compression_freeze` isolation test for this GUC behaviour, except session 2 starts a transaction rather than using debug_waitpoints
# This ensures that we can actually test the spin lock

use strict;
use warnings;
use TimescaleNode;
use Data::Dumper;
use Test::More;

# Test setup

# Create node with timescaledb
my $node = TimescaleNode->create('node');

# Create table
my $result = $node->safe_psql(
	'postgres', q{
  CREATE TABLE sensor_data (
	time timestamptz not null,
	sensor_id integer not null,
	cpu double precision null,
	temperature double precision null);
	}
);
is($result, '', 'create table');

# Create hypertable
$result = $node->safe_psql(
	'postgres', q{
   SELECT FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '1 month');
	}
);
is($result, '', 'create hypertable');

# Insert data
$result = $node->safe_psql(
	'postgres', q{
   INSERT INTO sensor_data
   SELECT
   time + (INTERVAL '1 minute' * random()) AS time,
   sensor_id,
   random() AS cpu,
   random()* 100 AS temperature
   FROM
   generate_series('2022-01-01', '2022-01-14', INTERVAL '1 hour') AS g1(time),
   generate_series(1, 50, 1) AS g2(sensor_id)
   ORDER BY time;
	}
);
is($result, '', 'insert data');

# Define count query
my $count_query = "SELECT count(*) FROM sensor_data;";

# Define compression query
my $compress_query =
  "SELECT count(*) FROM (SELECT compress_chunk(show_chunks('sensor_data')));";

# Count inserted rows
my $num_rows = $node->safe_psql('postgres', $count_query);

is($num_rows, 15650, 'validate inserted rows');

# Enable compression
$result = $node->safe_psql(
	'postgres', q{
   ALTER TABLE sensor_data SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id');
	}
);
is($result, '', 'enable compression');

# Get number of chunks
my $n_chunks = $node->safe_psql(
	'postgres', q{
SELECT count(*) FROM show_chunks('sensor_data');
});

# Create psql sessions
my $s1 = $node->background_psql('postgres');
my $s2 = $node->background_psql('postgres');

# SET session 1 behaviour to `truncate_or_delete`
$result = $s1->query_safe(
	"SET timescaledb.compress_truncate_behaviour TO truncate_or_delete;");
isnt($result, '', "session 1: set truncate_or_delete");

# Run tests

# TEST 1:
# Session 1 tries to truncate at the end of compression, but is blocked by session 2
# After the spin lock timeout is exceeded it falls back to delete instead

# Begin txns in both sessions
$result = $s1->query_safe("BEGIN");
isnt($result, '', "session 1: begin");

$result = $s2->query_safe("BEGIN");
isnt($result, '', "session 2: begin");

# The aggregation query takes an AccessShareLock on the chunk, preventing truncate
$result = $s2->query_safe($count_query);
is($result, $num_rows, "session 2: validate row count");

# Compress the chunk
# This tries and fails to acquire an AccessExclusiveLock on the table
# so after the spin-lock timeout it falls back to doing a delete
$result = $s1->query_safe($compress_query);
is($result, 1, "session 1: compress chunk");

$result = $s1->query_safe(
	"SELECT compression_status, count(*) FROM chunk_compression_stats('sensor_data') GROUP BY 1 ORDER BY 1, 2;"
);

my $expected = "Compressed|1";
is($result, $expected, "verify chunks are compressed");

$result = $node->safe_psql('postgres', $count_query);
is($result, $num_rows, "session 2: validate row count again");

$result = $s2->query_safe("COMMIT");
isnt($result, '', "session 2: commit");

# No AccessExclusiveLock on the uncompressed chunk
$result = $s1->query_safe(
	"SELECT relation::regclass::text FROM pg_locks WHERE granted AND relation::regclass::text LIKE '%hyper%chunk' AND locktype = 'relation' AND mode = 'AccessExclusiveLock' ORDER BY relation, locktype;"
);
$expected = "_timescaledb_internal.compress_hyper_2_2_chunk";
is($result, $expected, "verify AccessExclusiveLock was not taken");


$result = $s1->query_safe("ROLLBACK");
isnt($result, '', "session 1: rollback");

##########################################################################

# TEST 2:
# Session 1 is blocked by session 2 but session 2 commits before the spin-lock timeout is exceeded so session 1 truncates

# Begin txns in both sessions
$result = $s1->query_safe("BEGIN");
isnt($result, '', "session 1: begin");

$result = $s2->query_safe("BEGIN");
isnt($result, '', "session 2: begin");

$result = $s2->query_safe($count_query);
is($result, $num_rows, "session 2: validate row count");

# We have to use 'query_until('', ...)' so that the test immediately fires the next query
# Otherwise s1 times out and performs a delete
$s1->query_until('', $compress_query);

# Session 2 immediately commits, releasing the AccessShareLock on the table
$result = $s2->query_safe("COMMIT");
isnt($result, '', "session 2: commit");

# The result from the previous query_until() is returned with the next query
# so perform a dummy query on s1 to discard the result
# There might be a better way of doing this...
$s1->query_safe("SELECT 1");

$result = $s1->query_safe(
	"SELECT compression_status, count(*) FROM chunk_compression_stats('sensor_data') GROUP BY 1 ORDER BY 1, 2;"
);
is($result, 'Compressed|1', "verify chunks are compressed");

# AccessExclusiveLock taken on the uncompressed chunk
$result = $s1->query_safe(
	"SELECT relation::regclass::text FROM pg_locks WHERE granted AND relation::regclass::text LIKE '%hyper%chunk' AND locktype = 'relation' AND mode = 'AccessExclusiveLock' ORDER BY relation, locktype;"
);

$expected = "_timescaledb_internal._hyper_1_1_chunk
_timescaledb_internal.compress_hyper_2_3_chunk";
is($result, $expected, "verify AccessExclusiveLock was taken");

$result = $s1->query_safe("ROLLBACK");
isnt($result, '', "session 1: rollback");

$s1->quit();
$s2->quit();

done_testing();

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# This TAP test verifies the behavior when recompression is attempted on a chunk
# with unique constraints while concurrent DML is happening

use strict;
use warnings;
use TimescaleNode;
use Data::Dumper;
use Test::More;

# Test setup
my $node = TimescaleNode->create('node');

# Create table with a unique constraint
my $result = $node->safe_psql(
	'postgres', q{
    CREATE TABLE sensor_data (
        time timestamptz not null,
        sensor_id integer not null,
        cpu double precision null,
        temperature double precision null,
        UNIQUE(time, sensor_id)
    );
    }
);
is($result, '', 'create table with unique constraint');

# Create hypertable
$result = $node->safe_psql(
	'postgres', q{
    SELECT table_name FROM create_hypertable('sensor_data','time', chunk_time_interval => INTERVAL '1 month');
    }
);
is($result, 'sensor_data', 'create hypertable');

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
    generate_series('2022-01-01', '2022-01-07', INTERVAL '1 hour') AS g1(time),
    generate_series(1, 50, 1) AS g2(sensor_id)
    ORDER BY time;
    }
);
is($result, '', 'insert data');

# Define count query
my $count_query = "SELECT count(*) FROM sensor_data;";

# Count inserted rows
my $num_rows = $node->safe_psql('postgres', $count_query);
is($num_rows, 7250, 'validate inserted rows');

# Enable compression
$result = $node->safe_psql(
	'postgres', q{
    ALTER TABLE sensor_data SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'sensor_id',
        timescaledb.compress_orderby = 'time'
    );
    }
);
is($result, '', 'enable compression');

# Compress the chunk
my $compress_query =
  "SELECT count(*) FROM (SELECT compress_chunk(show_chunks('sensor_data')));";
$result = $node->safe_psql('postgres', $compress_query);
is($result, '1', 'compress chunk');

# Insert more data to make the chunk partial
$result = $node->safe_psql(
	'postgres', q{
    INSERT INTO sensor_data
    SELECT
    time + (INTERVAL '1 minute' * random()) AS time,
    sensor_id,
    random() AS cpu,
    random()* 100 AS temperature
    FROM
generate_series('2022-01-01', '2022-01--7', INTERVAL '1 hour') AS g1(time),
    generate_series(51, 55, 1) AS g2(sensor_id)
    ORDER BY time;
    }
);
is($result, '', 'insert more data to make the chunk partial');

# Create psql sessions
my $s1 = $node->background_psql('postgres');
my $s2 = $node->background_psql('postgres');

# Enable segmentwise recompression
$s1->query_safe("SET timescaledb.enable_segmentwise_recompression TO on;");

# Enable waiting to acquire an exclusive lock
$s1->query_safe("SET timescaledb.enable_recompress_waiting TO on;");

# TEST 1:
# Session 1 tries to acquire an exclusive lock at the end of recompression but is blocked due to the inserts by Session 2
# However
# Begin txns in both sessions

$result = $s1->query_safe("BEGIN;");

$result = $s2->query_safe("BEGIN;");

# Get lock data
$result = $node->safe_psql('postgres',
	"SELECT relation::regclass::text, mode FROM pg_locks WHERE relation::regclass::text LIKE '%hyper_1_%chunk' and granted;"
);
is($result, '', "verify no locks exist on the chunk");

# Session 2: Insert rows into the chunk
# This blocks until session 2 releases its locks
$s2->query_until(
	'', q{
    INSERT INTO sensor_data VALUES
    ('2022-01-05 12:00:00', 100, 0.5, 25.5),
    ('2022-01-05 13:00:00', 101, 0.6, 26.5);
});

# We have to use 'query_until('', ...)' so that the test immediately fires the next query
# Otherwise s1 times out throws an error
$s1->query_until(
	'', q{
    SELECT compress_chunk(show_chunks('sensor_data'));
});

# Session 2 immediately aborts, releasing the RowExclusiveLock on the table
$result = $s2->query_safe("ABORT");

# Verify ExclusiveLock on uncompressed chunk
$result = $node->safe_psql('postgres',
	"SELECT relation::regclass::text FROM pg_locks WHERE relation::regclass::text LIKE '%hyper_1_%chunk' AND granted AND mode = 'ExclusiveLock';"
);
is( $result,
	'_timescaledb_internal._hyper_1_1_chunk',
	"verify ExclusiveLock on uncompressed chunk");

# Verify AccessShareLock on compressed chunk
$result = $node->safe_psql('postgres',
	"SELECT relation::regclass::text FROM pg_locks WHERE relation::regclass::text LIKE '%hyper_1_%chunk' AND granted AND mode = 'AccessShareLock'"
);
is( $result,
	"_timescaledb_internal._hyper_1_1_chunk",
	"verify AccessShareLock on internal compressed chunk");

# Clean up
$result = $s1->query_safe("ROLLBACK;");

$s2->quit();
$s1->quit();

done_testing();

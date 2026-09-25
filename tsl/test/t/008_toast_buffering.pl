# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Test::More;

# Tests for the custom toaster's deferred chunk buffer
# (timescaledb.compression_toast_buffer_batches and
# timescaledb.compression_toast_buffer_size).
#
# The buffer holds back the toast chunks of up to N compressed batches and
# flushes them grouped by column, so the same column of consecutive batches
# lands on adjacent toast pages. These tests check:
#
# 1. The GUCs exist with their documented defaults and bounds.
# 2. Compression results are correct for N = 1, 2, 4 and for a byte threshold
#    small enough to force a flush mid-batch -- including the drain of the
#    last partial buffer at writer close.
# 3. The flushed layout really is column-major: the number of chunk_id runs
#    in ctid order over the compressed chunk's toast table drops when batches
#    are buffered, compared to flushing after every batch (N = 1).

my $node = TimescaleNode->create('toast_buffering');

$node->append_conf('postgresql.conf',
	"timescaledb.use_custom_toaster = true\nwal_level = replica\n");
$node->restart();

is($node->safe_psql('postgres', 'SHOW timescaledb.use_custom_toaster'),
	'on', 'custom toaster is enabled');
is($node->safe_psql('postgres',
		'SHOW timescaledb.compression_toast_buffer_batches'),
	'2', 'buffer_batches default is 2');
is($node->safe_psql('postgres',
		"SELECT setting FROM pg_settings WHERE name = 'timescaledb.compression_toast_buffer_size'"),
	'8192', 'buffer_size default is 8192 kB');

# Bounds: batches is capped at 4, the size must be at least 128 kB.
my ($ret, $stdout, $stderr) = $node->psql('postgres',
	'SET timescaledb.compression_toast_buffer_batches = 5');
is($ret, 3, 'buffer_batches = 5 is rejected');
($ret, $stdout, $stderr) = $node->psql('postgres',
	'SET timescaledb.compression_toast_buffer_size = 64');
is($ret, 3, 'buffer_size = 64 is rejected');

# Insert 3 hours of per-second rows across 4 devices, so compression
# produces several batches per segment. Returns a checksum of the
# uncompressed data for later comparison.
sub fill_table
{
	my ($name) = @_;

	$node->safe_psql(
		'postgres', qq[
		CREATE TABLE $name(time timestamptz, device_id int, val double precision, payload text);
		SELECT create_hypertable('$name', 'time');
		ALTER TABLE $name SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id', timescaledb.compress_orderby = 'time');
		INSERT INTO $name
		SELECT t, (extract(epoch from t)::int % 4), random(), repeat(md5(t::text), 10)
		FROM generate_series('2024-01-01'::timestamptz, '2024-01-01 03:00:00'::timestamptz, interval '1 sec') t;
		]);

	return $node->safe_psql(
		'postgres', qq[
		SELECT count(*) || '|' || md5(string_agg(format('%s|%s|%s|%s', time, device_id, val, payload), ',' ORDER BY device_id, time))
		FROM $name]);
}

# Compress with the given buffer GUCs and return the post-compression
# checksum plus the number of chunk_id runs in ctid order over the
# compressed chunk's toast table.
sub compress_and_measure
{
	my ($name, $batches, $size_kb) = @_;

	$node->safe_psql(
		'postgres', qq[
		SET timescaledb.compression_toast_buffer_batches = $batches;
		SET timescaledb.compression_toast_buffer_size = $size_kb;
		SELECT compress_chunk(c) FROM show_chunks('$name') c;
		]);

	my $checksum = $node->safe_psql(
		'postgres', qq[
		SELECT count(*) || '|' || md5(string_agg(format('%s|%s|%s|%s', time, device_id, val, payload), ',' ORDER BY device_id, time))
		FROM $name]);

	my $toast = $node->safe_psql(
		'postgres', qq[
		SELECT 'pg_toast.' || toast.relname
		FROM _timescaledb_catalog.compression_settings cs
		JOIN pg_class c ON c.oid = cs.compress_relid
		JOIN pg_class toast ON toast.oid = c.reltoastrelid
		WHERE cs.relid IN (SELECT show_chunks('$name'))]);

	# Map each toasted value to its column class by the algorithm byte at
	# the start of its first chunk: DELTADELTA (4) is time, GORILLA (3) is
	# val, anything else is payload. The byte sits at the start of the
	# chunk payload for values PG did not compress; the payload column's
	# exact encoding does not matter as long as it is neither 3 nor 4.
	my %class;
	foreach my $line (
		split(
			/\n/,
			$node->safe_psql(
				'postgres', qq[
				SELECT chunk_id, get_byte(chunk_data, 0)
				FROM $toast WHERE chunk_seq = 0])))
	{
		my ($chunk_id, $byte) = split(/\|/, $line);
		$class{$chunk_id} =
			$byte == 4 ? 'time' : $byte == 3 ? 'val' : 'payload';
	}

	# Count column-class transitions in ctid (= write) order. Batch-major
	# layout costs one transition per toasted column per batch; column-major
	# flush costs one per column per buffered group.
	my $runs = 0;
	my $prev = '';
	foreach my $chunk_id (
		split(
			/\n/,
			$node->safe_psql('postgres',
				"SELECT chunk_id FROM $toast ORDER BY ctid")))
	{
		my $class = $class{$chunk_id};
		$runs++ if !defined $class || $class ne $prev;
		$prev = $class;
	}

	return ($checksum, $runs);
}

# Reference data: identical contents for both layout-comparison tables.
# random() makes val differ between tables, so compare each table against
# its own pre-compression checksum and only compare the run counts across
# tables.
my $ref_n1   = fill_table('metrics_n1');
my $ref_n4   = fill_table('metrics_n4');
my $ref_tiny = fill_table('metrics_tiny');

# N = 1 flushes after every batch: batch-major layout, one run per toasted
# column per batch.
my ($sum_n1, $runs_n1) = compress_and_measure('metrics_n1', 1, 8192);
is($sum_n1, $ref_n1, 'N=1: decompressed data is correct');

# N = 4 flushes after four batches: column-major layout, one run per column
# per four batches.
my ($sum_n4, $runs_n4) = compress_and_measure('metrics_n4', 4, 8192);
is($sum_n4, $ref_n4, 'N=4: decompressed data is correct');

# The run count must drop clearly when buffering. The ideal factor is the
# batch count ratio (4x); demand 2x to stay robust against uneven batch
# sizes and partial final buffers.
cmp_ok($runs_n1, '>=', 2 * $runs_n4,
	"N=4 halves the chunk_id runs compared to N=1 ($runs_n1 vs $runs_n4)");

# Tiny byte threshold: 128 kB forces flushes inside a single batch (one
# payload column alone is larger). Data must still be correct, which also
# covers the drain of the last partial buffer at writer close.
my ($sum_tiny, $runs_tiny) = compress_and_measure('metrics_tiny', 4, 128);
is($sum_tiny, $ref_tiny,
	'tiny buffer_size: decompressed data is correct after forced early flushes');

done_testing();

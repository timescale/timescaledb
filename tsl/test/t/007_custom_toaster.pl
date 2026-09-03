# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Test::More;

# Two things about the custom compression toaster are checked here, by looking
# at what compression actually wrote to WAL:
#
# 1. It stays off when wal_level is logical. It writes a value's toast chunks
#    with one heap_multi_insert() call, and only heap_insert() marks a toast
#    chunk insert as such in the WAL -- the mark logical decoding needs to
#    collect the chunks and stitch the value back together.
#
# 2. Where it does run, the WAL it writes replays correctly: compress a chunk,
#    crash the server before a checkpoint can flush the new pages to disk, and
#    the compressed chunk still decompresses to the same data afterwards.

my $node = TimescaleNode->create('custom_toaster');

if ($node->safe_psql('postgres',
		q[SELECT count(*) FROM pg_available_extensions WHERE name = 'pg_walinspect']
	) ne '1')
{
	plan skip_all => 'pg_walinspect is not available';
}

$node->safe_psql('postgres', 'CREATE EXTENSION pg_walinspect');

# Push the checkpointer's own schedule well past how long this test takes, so a
# timed checkpoint can't sneak in and flush the compressed chunk's pages before
# the crash at the end -- we verify below (via pg_control_checkpoint()) that
# none completed, but this makes that outcome the expected one rather than a
# race.
#
# The shared test config runs at wal_level = replica so that the custom toaster
# gets exercised, so ask for logical explicitly for the first part below.
$node->append_conf('postgresql.conf',
	"checkpoint_timeout = '1h'\nmax_wal_size = '10GB'\ntimescaledb.use_custom_toaster = true\nwal_level = logical\n"
);
$node->restart();

# Compress a hypertable whose payload column is wide enough that every
# compressed batch has to be toasted, and return how many WAL records of each
# kind landed in the compressed chunk's toast relation.
sub compress_and_count_toast_wal
{
	my ($name) = @_;

	my $start_lsn =
	  $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

	$node->safe_psql(
		'postgres', qq[
		CREATE TABLE $name(time timestamptz, device_id int, val double precision, payload text);
		SELECT create_hypertable('$name', 'time');
		ALTER TABLE $name SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id', timescaledb.compress_orderby = 'time');
		INSERT INTO $name
		SELECT t, (extract(epoch from t)::int % 4), random(), repeat(md5(t::text), 10)
		FROM generate_series('2024-01-01'::timestamptz, '2024-01-01 01:00:00'::timestamptz, interval '1 sec') t;
		SELECT compress_chunk(c) FROM show_chunks('$name') c;
		]);

	my $end_lsn =
	  $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

	my $toast_filenodes = $node->safe_psql(
		'postgres', qq[
		SELECT string_agg(toast.relfilenode::text, ',')
		FROM _timescaledb_catalog.compression_settings cs
		JOIN pg_class c ON c.oid = cs.compress_relid
		JOIN pg_class toast ON toast.oid = c.reltoastrelid
		WHERE cs.relid IN (SELECT show_chunks('$name'))
		]);

	isnt($toast_filenodes, '',
		"$name: compressed chunk has a toast relation");

	my %counts;
	foreach my $line (
		split(
			/\n/,
			$node->safe_psql(
				'postgres', qq[
		SELECT record_type, count(*)
		FROM pg_get_wal_block_info('$start_lsn', '$end_lsn', false)
		WHERE relfilenode IN ($toast_filenodes)
		GROUP BY record_type
		])))
	{
		my ($record_type, $count) = split(/\|/, $line);
		$counts{$record_type} = $count;
	}

	return \%counts;
}

# The val column is random, so compare the whole table against itself over the
# crash rather than against fixed values.
sub checksum
{
	my ($name) = @_;

	return $node->safe_psql(
		'postgres', qq[
		SELECT count(*), md5(string_agg(format('%s|%s|%s|%s', time, device_id, val, payload), ',' ORDER BY device_id, time))
		FROM $name]);
}

# The payload is derived from the time, so this checks the data is right and not
# just unchanged.
sub payload_intact
{
	my ($name) = @_;

	return $node->safe_psql(
		'postgres', qq[
		SELECT count(*) = 3601 AND count(*) = count(payload)
		FROM $name WHERE payload = repeat(md5(time::text), 10)]);
}

is($node->safe_psql('postgres', 'SHOW wal_level'),
	'logical', 'wal_level is logical after restart');
is($node->safe_psql('postgres', 'SHOW timescaledb.use_custom_toaster'),
	'on', 'custom toaster is enabled');

my $logical_counts = compress_and_count_toast_wal('metrics_logical');

is($logical_counts->{MULTI_INSERT},
	undef,
	'wal_level = logical: no toast chunk written by the custom toaster');
cmp_ok($logical_counts->{INSERT} // 0,
	'>', 0, 'wal_level = logical: toast chunks written by core instead');

# Drop to wal_level = replica, where nothing can be decoding, and the custom
# toaster takes over. This is also the control for the check above -- without
# it, a broken query in the helper would look like a passing test.
$node->append_conf('postgresql.conf', "wal_level = replica\n");
$node->restart();

is($node->safe_psql('postgres', 'SHOW wal_level'),
	'replica', 'wal_level is back to replica');

# redo_lsn only advances when a checkpoint completes, and pg_control_checkpoint()
# has the same column names on every supported PG version, unlike
# pg_stat_checkpointer (whose columns differ between PG17 and PG18). Read it
# after the restart above, whose shutdown checkpoint advances it.
my $redo_lsn_query = q[SELECT redo_lsn FROM pg_control_checkpoint()];
my $redo_lsn_at_start = $node->safe_psql('postgres', $redo_lsn_query);

my $replica_counts = compress_and_count_toast_wal('metrics_replica');

cmp_ok($replica_counts->{MULTI_INSERT} // 0,
	'>', 0,
	'wal_level = replica: toast chunks written by the custom toaster');

my %checksum_before =
  map { $_ => checksum($_) } ('metrics_logical', 'metrics_replica');

is($node->safe_psql('postgres', $redo_lsn_query),
	$redo_lsn_at_start,
	'no checkpoint completed between server start and the crash below');

# "immediate" shutdown skips the checkpoint a clean stop does, so recovery has
# to replay the WAL from the compression above -- the same WAL the custom
# toaster generated -- to reconstruct the compressed chunk's pages.
$node->stop('immediate');
$node->start();

is($node->safe_psql('postgres', 'SELECT 1'),
	'1', 'server recovered after crash');

foreach my $name ('metrics_logical', 'metrics_replica')
{
	is(checksum($name), $checksum_before{$name},
		"$name: decompressed data unchanged after crash recovery");
	is(payload_intact($name), 't', "$name: decompressed data is correct");
}

done_testing();

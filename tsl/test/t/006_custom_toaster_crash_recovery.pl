# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Test::More;

# This test checks that the WAL our custom compression toaster
# generates replays correctly during crash recovery: compress
# a chunk with the custom toaster on, crash the server before a checkpoint can
# flush the new pages to disk, and confirm the compressed chunk still
# decompresses to the same data after recovery.

my $node = TimescaleNode->create('custom_toaster_crash_recovery');

# Push the checkpointer's own schedule well past how long this test takes, so
# a timed checkpoint can't sneak in and flush the compressed chunk's pages
# before the crash -- we verify below (via pg_control_checkpoint()) that none
# completed, but this makes that outcome the expected one rather than a race.
$node->append_conf('postgresql.conf',
	"checkpoint_timeout = '1h'\nmax_wal_size = '10GB'\ntimescaledb.use_custom_toaster = true\n"
);
$node->reload();

# redo_lsn only advances when a checkpoint completes, and pg_control_checkpoint()
# has the same column names on every supported PG version, unlike
# pg_stat_checkpointer (whose columns differ between PG17 and PG18).
my $redo_lsn_query    = q[SELECT redo_lsn FROM pg_control_checkpoint()];
my $redo_lsn_at_start = $node->safe_psql('postgres', $redo_lsn_query);

$node->safe_psql(
	'postgres', q[
	CREATE TABLE metrics(time timestamptz, device_id int, val double precision, payload text);
	SELECT create_hypertable('metrics', 'time');
	ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id', timescaledb.compress_orderby = 'time');
	INSERT INTO metrics
	SELECT t, (extract(epoch from t)::int % 4), random(), repeat(md5(t::text), 10)
	FROM generate_series('2024-01-01'::timestamptz, '2024-01-01 01:00:00'::timestamptz, interval '1 sec') t;
	SELECT compress_chunk(c) FROM show_chunks('metrics') c;
	]);

my $checksum_query = q[
	SELECT count(*), md5(string_agg(format('%s|%s|%s|%s', time, device_id, val, payload), ',' ORDER BY device_id, time))
	FROM metrics];

my $checksum_before = $node->safe_psql('postgres', $checksum_query);

my $redo_lsn_before_crash = $node->safe_psql('postgres', $redo_lsn_query);
is($redo_lsn_before_crash, $redo_lsn_at_start,
	'no checkpoint completed between server start and the crash below');

# "immediate" shutdown skips the checkpoint a clean stop does, so recovery
# has to replay the WAL from the compress_chunk() above -- the same WAL the
# custom toaster generated -- to reconstruct the compressed chunk's pages.
$node->stop('immediate');
$node->start();

is($node->safe_psql('postgres', 'SELECT 1'),
	'1', 'server recovered after crash');

my $checksum_after = $node->safe_psql('postgres', $checksum_query);

is($checksum_after, $checksum_before,
	'decompressed data unchanged after crash recovery');

done_testing();

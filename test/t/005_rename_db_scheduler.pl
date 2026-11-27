# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# Renaming a database must not fail because of its own scheduler. If the
# scheduler is started again before the rename, it connects to the database
# and waits for the renaming transaction, and the rename fails with "is being
# accessed by other users". A lock on pg_database holds the rename long
# enough for the scheduler to connect.

use strict;
use warnings;
use TimescaleNode;
use Test::More;
use Time::HiRes qw(usleep);

my $node = TimescaleNode->create('rename_db');

$node->safe_psql('postgres',  'CREATE DATABASE rename_db');
$node->safe_psql('rename_db', 'CREATE EXTENSION timescaledb');
my $db_oid = $node->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'rename_db'");

my $scheduler_query = qq{
SELECT count(*) FROM pg_stat_activity
WHERE datid = $db_oid
  AND application_name = 'TimescaleDB Background Worker Scheduler'};

$node->poll_query_until('postgres', "SELECT ($scheduler_query) = 1")
  or die "scheduler did not start";

# Hold the rename on the pg_database lock, after the scheduler messages
my $locker = $node->background_psql('postgres');
$locker->query_safe('BEGIN');
$locker->query_safe('LOCK TABLE pg_database IN SHARE MODE');

my $renamer = $node->background_psql('postgres', on_error_stop => 0);
$renamer->query_until(qr/start/,
	"\\echo start\nALTER DATABASE rename_db RENAME TO rename_db_new;\n");

$node->poll_query_until(
	'postgres', q{
SELECT count(*) > 0 FROM pg_locks
WHERE relation = 'pg_database'::regclass AND NOT granted}
) or die "rename did not wait for the lock";

# Give a restarted scheduler time to connect. It takes milliseconds, so
# without the fix this always happens before the lock is released.
for (my $i = 0; $i < 50; $i++)
{
	last if $node->safe_psql('postgres', $scheduler_query) > 0;
	usleep(100_000);
}

$locker->query_safe('COMMIT');
$renamer->query('SELECT 1');
is($renamer->{stderr}, '', 'rename has no error');
is( $node->safe_psql(
		'postgres', "SELECT datname FROM pg_database WHERE oid = $db_oid"),
	'rename_db_new',
	'database is renamed');

ok($node->poll_query_until('postgres', "SELECT ($scheduler_query) = 1"),
	'scheduler runs after rename');

# A failed rename must start the scheduler again too
$node->safe_psql('postgres', 'CREATE DATABASE rename_db_other');
my ($rc, $out, $err) = $node->psql('postgres',
	'ALTER DATABASE rename_db_new RENAME TO rename_db_other');
like($err, qr/already exists/, 'rename to an existing name fails');
ok($node->poll_query_until('postgres', "SELECT ($scheduler_query) = 1"),
	'scheduler runs after failed rename');

$locker->quit;
$renamer->quit;
$node->stop;
done_testing();

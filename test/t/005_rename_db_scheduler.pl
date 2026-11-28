# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# Renaming a database or moving it to another tablespace must not fail
# because of its own scheduler. If the scheduler is started again before the
# command, it connects to the database and waits for the command's
# transaction, and the command fails with "is being accessed by other users".
# A lock on pg_database holds the command long enough for the scheduler to
# connect.

use strict;
use warnings;
use TimescaleNode;
use Test::More;
use File::Spec;
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

# Run a command while it is held on the pg_database lock, after the
# scheduler messages are sent, and check that it has no error
sub run_held
{
	my ($cmd, $name) = @_;

	my $locker = $node->background_psql('postgres');
	$locker->query_safe('BEGIN');
	$locker->query_safe('LOCK TABLE pg_database IN SHARE MODE');

	my $session = $node->background_psql('postgres', on_error_stop => 0);
	$session->query_until(qr/start/, "\\echo start\n$cmd;\n");

	$node->poll_query_until(
		'postgres', q{
SELECT count(*) > 0 FROM pg_locks
WHERE relation = 'pg_database'::regclass AND NOT granted}
	) or die "$name did not wait for the lock";

	# Give a restarted scheduler time to connect. It takes milliseconds, so
	# without the fix this always happens before the lock is released.
	for (my $i = 0; $i < 50; $i++)
	{
		last if $node->safe_psql('postgres', $scheduler_query) > 0;
		usleep(100_000);
	}

	$locker->query_safe('COMMIT');
	$session->query('SELECT 1');
	is($session->{stderr}, '', "$name has no error");

	$locker->quit;
	$session->quit;
}

run_held('ALTER DATABASE rename_db RENAME TO rename_db_new', 'rename');
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

my $ts_dir = File::Spec->rel2abs(PostgreSQL::Test::Utils::tempdir());
$node->safe_psql('postgres', "CREATE TABLESPACE move_ts LOCATION '$ts_dir'");
run_held('ALTER DATABASE rename_db_new SET TABLESPACE move_ts',
	'tablespace change');
is( $node->safe_psql(
		'postgres', q{
SELECT spcname FROM pg_database d JOIN pg_tablespace t ON t.oid = d.dattablespace
WHERE datname = 'rename_db_new'}),
	'move_ts',
	'database is moved');
ok($node->poll_query_until('postgres', "SELECT ($scheduler_query) = 1"),
	'scheduler runs after tablespace change');

$node->stop;
done_testing();

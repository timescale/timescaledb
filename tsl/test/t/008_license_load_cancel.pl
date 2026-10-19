# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# A query cancel on the first statement of a session must not leave that
# session on the Apache cross-module stubs (#10711).
#
# The loader locks pg_extension to read the extension version, publishes
# that version, then locks pg_extension again from the versioned library's
# _PG_init. Two sessions pass an AccessExclusiveLock forward so each of
# those acquisitions can complete except the one after the versioned
# library is mapped. The cancel lands there, after the version was
# published and before TSL init.

use strict;
use warnings;
use TimescaleNode;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'need /proc/<pid>/maps to see when the versioned library is mapped'
  unless -r '/proc/self/maps';

my $node = TimescaleNode->create('node');

$node->safe_psql(
	'postgres', q{
    CREATE TABLE measurements (
        time timestamptz NOT NULL,
        device_id integer NOT NULL,
        value double precision NOT NULL,
        PRIMARY KEY (device_id, time)
    );
    SELECT create_hypertable('measurements', 'time');
    CREATE MATERIALIZED VIEW measurements_1min WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 minute', time) AS bucket, device_id, avg(value) AS value
        FROM measurements GROUP BY 1, 2 WITH NO DATA;
    }
);

my $upsert = q{
    INSERT INTO measurements (time, device_id, value)
    VALUES ('2026-09-29 13:00:00+00', 7, 3.5)
    ON CONFLICT (device_id, time) DO UPDATE SET value = EXCLUDED.value;
};

sub connect_session
{
	my ($appname, %params) = @_;
	local $ENV{PGAPPNAME} = $appname;
	return $node->background_psql('postgres', %params);
}

sub nudge
{
	my ($session) = @_;
	return unless $session->{run}->pumpable();
	$session->{run}->pump_nb();
}

my $extension_oid;

sub extension_lock_held
{
	my ($pid, $granted) = @_;
	my $grant = $granted ? '' : 'NOT ';
	my $mode = $granted ? "AND mode = 'AccessExclusiveLock'" : '';

	# Compare the relation OID directly. Casting pg_extension to regclass
	# takes AccessShareLock on it and would queue behind the exclusive lock
	# this test is holding.
	return qq{EXISTS (
            SELECT 1 FROM pg_locks
            WHERE pid = $pid
              AND relation = $extension_oid
              AND ${grant}granted
              $mode
        )};
}

sub poll_true
{
	my ($session, $sql, $what, @pump) = @_;
	my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
	my $got = '';
	while (time() < $deadline)
	{
		nudge($_) for @pump;
		$got = $session->query_safe($sql);
		return if $got eq 't';
		usleep(20_000);
	}
	my $activity = $session->query_safe(
		q{SELECT pid, application_name, state, wait_event_type, wait_event
          FROM pg_stat_activity
          WHERE application_name LIKE 'license_load%'
          ORDER BY application_name});
	die "$what (last=$got)\n$activity";
}

sub library_mapped
{
	my ($pid) = @_;
	my $path = "/proc/$pid/maps";
	return 'unknown' unless -r $path;
	open my $fh, '<', $path or return 'unknown';
	local $/ = undef;
	my $text = <$fh>;
	close $fh;
	return 'tsl' if $text =~ m{/timescaledb-tsl-\d};
	return 'main' if $text =~ m{/timescaledb-\d};
	return 'none';
}

my $owner = connect_session('license_load_owner');
my $standby = connect_session('license_load_standby');

# Every normal statement locks pg_extension while the loader reads the
# extension version. The controlling sessions must not do that, or their
# polls queue behind the exclusive lock they are using as the barrier.
$owner->query_safe('SET timescaledb.disable_load = on');
$standby->query_safe('SET timescaledb.disable_load = on');
my $owner_pid = $owner->query_safe('SELECT pg_backend_pid()');
my $standby_pid = $standby->query_safe('SELECT pg_backend_pid()');
$extension_oid = $owner->query_safe("SELECT 'pg_extension'::regclass::oid");
chomp($owner_pid, $standby_pid, $extension_oid);

my $victim = connect_session('license_load_victim', on_error_stop => 0);
my $victim_pid = $owner->query_safe(
	q{SELECT pid FROM pg_stat_activity WHERE application_name = 'license_load_victim'}
);
chomp($victim_pid);
isnt($victim_pid, '', 'victim backend is connected');

$owner->query_safe('BEGIN');
$owner->query_safe(
	'LOCK TABLE pg_catalog.pg_extension IN ACCESS EXCLUSIVE MODE');
$victim->query_until('', "$upsert\n");
poll_true($owner, 'SELECT ' . extension_lock_held($victim_pid, 0),
	'victim waits on the first pg_extension lock',
	$victim);

my $steps = 0;
while ($steps++ < 6)
{
	my $mapped = library_mapped($victim_pid);
	last if $mapped eq 'main';
	BAIL_OUT('TSL library mapped before the cancel landed')
	  if $mapped eq 'tsl';

	$standby->query_until('',
		"BEGIN;\nLOCK TABLE pg_catalog.pg_extension IN ACCESS EXCLUSIVE MODE;\n"
	);
	poll_true($owner, 'SELECT ' . extension_lock_held($standby_pid, 0),
		'standby queued behind the victim',
		$victim, $standby);
	$owner->query_safe('COMMIT');
	poll_true(
		$owner,
		'SELECT '
		  . extension_lock_held($standby_pid, 1) . ' AND '
		  . extension_lock_held($victim_pid, 0),
		'victim waits on the next pg_extension lock',
		$victim, $standby);
	# Consume the LOCK result so this session can run SQL while holding it.
	$standby->query_safe('SELECT 1');
	($owner, $standby) = ($standby, $owner);
	($owner_pid, $standby_pid) = ($standby_pid, $owner_pid);
}

is(library_mapped($victim_pid), 'main',
	'cancel lands after the versioned library is mapped');

$standby->query_safe("SELECT pg_cancel_backend($victim_pid)");
poll_true(
	$owner,
	'SELECT NOT '
	  . extension_lock_held($victim_pid, 0)
	  . qq{ AND EXISTS (
              SELECT 1 FROM pg_stat_activity
              WHERE pid = $victim_pid AND state = 'idle')},
	'cancelled victim leaves the pg_extension wait',
	$victim);
$owner->query_safe('COMMIT');

is($victim->query('SHOW timescaledb.license'),
	'timescale', 'license is unchanged after the cancel');
is($victim->query('SELECT _timescaledb_functions.tsl_loaded()'),
	't', 'TSL module is loaded on the cancelled connection');
like($victim->{stderr},
	qr/canceling statement due to user request/,
	'first statement was cancelled');
unlike(
	$victim->{stderr},
	qr/not supported under the current/,
	'cancelled statement does not hit the Apache stubs');

$victim->query($upsert);
unlike(
	$victim->{stderr},
	qr/not supported under the current/,
	'upsert succeeds on the cancelled connection');
is($victim->query('SELECT count(*) FROM measurements'),
	'1', 'upsert wrote the row');

$victim->quit();
$standby->quit();
$owner->quit();

done_testing();

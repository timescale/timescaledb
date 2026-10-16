# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# A query cancel that arrives while the first statement of a session is
# loading the TSL module must not leave that session on the Apache
# cross-module stubs (#10711).

use strict;
use warnings;
use TimescaleNode;
use Test::More;
use Time::HiRes qw(usleep);

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

# Every backend that loads the extension while the wait point is enabled
# blocks on it, so the controlling session loads it first.
my $ctl = $node->background_psql('postgres');
$ctl->query_safe(
	"SELECT debug_waitpoint_enable('license_enable_module_loading');");

# Nothing has run in this session yet, so the upsert is what loads the
# extension and stops at the wait point. application_name has to be set
# before that statement. background_psql accepts connstr only since
# PostgreSQL 18, so set PGAPPNAME, which libpq applies on every version.
my $victim;
{
	local $ENV{PGAPPNAME} = 'license_load_cancel';
	$victim = $node->background_psql('postgres', on_error_stop => 0);
}
$victim->query_until('', "$upsert\n");

my $waiting_query = q{
    SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a USING (pid)
    WHERE a.application_name = 'license_load_cancel'
      AND l.locktype = 'advisory' AND NOT l.granted;
};

my $pid      = '';
my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
while ($pid eq '' && time() < $deadline)
{
	$pid = $ctl->query_safe($waiting_query);
	usleep(100_000) if $pid eq '';
}
isnt($pid, '', 'first statement blocks while loading the TSL module');

$ctl->query_safe("SELECT pg_cancel_backend($pid);");

# A cancel that can interrupt the wait does so promptly. Give it the chance
# before releasing the wait point.
$deadline = time() + 2;
while ($ctl->query_safe($waiting_query) ne '' && time() < $deadline)
{
	usleep(100_000);
}
$ctl->query_safe(
	"SELECT debug_waitpoint_release('license_enable_module_loading');");

is($victim->query('SELECT _timescaledb_functions.tsl_loaded();'),
	't', 'TSL module is loaded after the cancel');
my $first_stderr = $victim->{stderr};
unlike(
	$first_stderr,
	qr/not supported under the current/,
	'cancelled statement does not hit the Apache stubs');

$victim->query($upsert);
is($victim->{stderr}, $first_stderr,
	'upsert succeeds on the cancelled connection');
is($ctl->query_safe('SELECT count(*) FROM measurements;'),
	'1', 'upsert wrote the row');

$victim->quit();
$ctl->quit();

done_testing();

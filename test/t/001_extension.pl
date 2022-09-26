# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Data::Dumper;
use Test::More tests => 6;

# This test checks that the extension state is handled correctly
# across multiple sessions. Specifically, if the extension state
# changes in one session (e.g., the extension is created or dropped),
# this should be reflected in other concurrent sessions.
#
# To test this, we start one background psql session that stays open
# for the duration of the tests and then change the extension state
# from other sessions.
my $node  = TimescaleNode->create('extension');
my $in    = '';
my $out   = '';
my $timer = IPC::Run::timeout(180);
my $h =
  $node->background_psql('postgres', \$in, \$out, $timer, on_error_stop => 0);

sub check_extension_state
{
	my ($pattern, $annotation) = @_;

	# report test failures from caller location
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	# reset output collector
	$out = "";
	# restart per-command timer
	$timer->start(5);
	# send the data to be sent
	$in .= "SELECT * FROM extension_state();\n";
	# wait ...

	pump $h until ($out =~ $pattern || $timer->is_expired);
	my $okay = ($out =~ $pattern && !$timer->is_expired);
	ok($okay, $annotation);
	# for debugging, log actual output if it didn't match
	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Useqq = 1;
	diag 'Actual output was ' . Dumper($out) . "Did not match \"$pattern\"\n"
	  if !$okay;
	return;
}

# Create extension_state function and check initial state
my $result = $node->safe_psql(
	'postgres',
	q{
	\ir t/functions.sql
	SELECT * FROM extension_state();
});

# Initially, the state should be "created" in both sessions
is($result, qq/created/, "initial state is \"created\"");
check_extension_state(qr/created/, "initial state is \"created\"");

# Drop the extension in one session, and the new state should be
# reflected in the other backend.
$result = $node->safe_psql(
	'postgres', q{
	DROP EXTENSION timescaledb;
	SELECT * FROM extension_state();
});

# After dropping the extension, the new state in both sessions should
# be "unknown"
is($result, qq/unknown/, "state after dropped is \"unknown\"");
check_extension_state(qr/unknown/,
	"state is \"unknown\" after extension is dropped in other backend");

# Create the extension again, which should be reflected in both
# sessions.
$result = $node->safe_psql(
	'postgres', q{
	CREATE EXTENSION timescaledb;
	SELECT * FROM extension_state();
});

# After creating the extension again in one session, the other session
# should go back to "created" state.
is($result, qq/created/, "state after creating extension is \"created\"");
check_extension_state(qr/created/,
	"state is \"created\" after extension is created in other backend");

# Quit the interactive psql session
$in .= q{
	\q
};

$h->finish or die "psql returned $?";
$node->stop;

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use TestLib;
use Data::Dumper;
use Test::More tests => 4;

# This test checks that the extension state is handled correctly
# across multiple sessions. Specifically, if the extension state
# changes in one session (e.g., the extension is created or dropped),
# this should be reflected in other concurrent sessions.
#
# To test this, we start one background psql session that stays open
# for the duration of the tests and then change the extension state
# from other sessions.
my $node_primary = TimescaleNode->create(
	'primary',
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby = PostgresNode->get_new_node('standby_1');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

# Wait for standby to catch up
$node_primary->wait_for_catchup($node_standby, 'replay',
	$node_primary->lsn('insert'));


my $result = $node_primary->safe_psql('postgres',
	"SELECT get_telemetry_report()->'replication'->>'num_wal_senders'");
is($result, qq(1), 'number of wal senders on primary');

$result = $node_primary->safe_psql('postgres',
	"SELECT get_telemetry_report()->'replication'->>'is_wal_receiver'");
is($result, qq(false), 'primary is wal receiver');

$result = $node_standby->safe_psql('postgres',
	"SELECT get_telemetry_report()->'replication'->>'num_wal_senders'");
is($result, qq(0), 'number of wal senders on standby');

$result = $node_standby->safe_psql('postgres',
	"SELECT get_telemetry_report()->'replication'->>'is_wal_receiver'");
is($result, qq(true), 'standby is wal receiver');

done_testing();

1;

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test a simple multi node cluster creation and basic operations
use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 1;

#Initialize all the multi-node instances
my $an  = AccessNode->create('an');
my $dn1 = DataNode->create('dn1');
my $dn2 = DataNode->create('dn2');

$an->add_data_node($dn1);
$an->add_data_node($dn2);

$dn1->append_conf('postgresql.conf', 'log_connections=true');
$dn2->append_conf('postgresql.conf', 'log_connections=true');

my $output = $an->safe_psql(
	'postgres',
	qq[
	CREATE ROLE alice;
	CALL distributed_exec('CREATE ROLE alice LOGIN');
	GRANT USAGE ON FOREIGN SERVER dn1,dn2 TO alice;
	SET ROLE alice;
	CREATE TABLE conditions (time timestamptz, location int, temp float);
	SELECT create_distributed_hypertable('conditions', 'time', 'location');
	INSERT INTO conditions VALUES ('2021-09-20 00:00:00+02', 1, 23.4);
    ]);


my ($cmdret, $stdout, $stderr) = $an->psql(
	'postgres',
	qq[
	SET ROLE alice;
	SELECT time AT TIME ZONE 'America/New_York', location, temp FROM conditions;
	SELECT node_name, user_name, invalidated
   	FROM _timescaledb_internal.show_connection_cache()
	WHERE user_name='alice';
	RESET ROLE;
	DROP TABLE conditions;
	REVOKE USAGE ON FOREIGN SERVER dn1,dn2 FROM alice;
	DROP ROLE ALICE;
	SELECT node_name, user_name, invalidated
   	FROM _timescaledb_internal.show_connection_cache()
	WHERE user_name='alice';
	
]);

# Expected output:
#
# * First row is from the SELECT query that creates the connections
# * for alice.
#
# * Second row is the output from the connection cache after SELECT
#   and prior to dropping alice. The entry has invalidated=false, so
#   the entry is still valid.
#
# * There is no row for the third connection cache query since the
# * connections for alice have been purged.
is( $stdout,
	q[2021-09-19 18:00:00|1|23.4
dn1|alice|f]);

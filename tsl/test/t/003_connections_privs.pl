# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 28;

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

# Test DROP OWNED BY and REASSIGN OWNED BY behavior across the setup
# Create a bunch of roles and use them to set up grant dependencies and
# object ownership
$an->safe_psql(
	'postgres',
	qq[
CREATE USER regress_dep_user0;
CREATE USER regress_dep_user1;
CREATE USER regress_dep_user2;
call distributed_exec(\$\$ CREATE USER regress_dep_user0; \$\$);
call distributed_exec(\$\$  CREATE USER regress_dep_user1; \$\$);
call distributed_exec(\$\$ CREATE USER regress_dep_user2;\$\$);
GRANT USAGE ON FOREIGN SERVER dn1,dn2 TO regress_dep_user0;
GRANT USAGE ON FOREIGN SERVER dn1,dn2 TO regress_dep_user1;
GRANT USAGE ON FOREIGN SERVER dn1,dn2 TO regress_dep_user2;
SET SESSION AUTHORIZATION regress_dep_user0;
CREATE TABLE conditions1 (time bigint NOT NULL, device int, temp float);
SELECT create_distributed_hypertable('conditions1', 'time', chunk_time_interval => 10);
GRANT ALL ON conditions1 TO regress_dep_user1 WITH GRANT OPTION;
SET SESSION AUTHORIZATION regress_dep_user1;
GRANT ALL ON conditions1 TO regress_dep_user2 WITH GRANT OPTION;
    ]);

# Check the access privileges an AN and DN
my $query =
  q[SELECT pg_catalog.array_to_string(c.relacl, E'\n') FROM pg_catalog.pg_class c WHERE c.relname = 'conditions1';];

for my $node ($an, $dn1, $dn2)
{
	$node->psql_is(
		'postgres', $query, q[regress_dep_user0=arwdDxt/regress_dep_user0
regress_dep_user1=a*r*w*d*D*x*t*/regress_dep_user0
regress_dep_user2=a*r*w*d*D*x*t*/regress_dep_user1],
		'Node shows correct set of access privileges');
}

# Execute DROP OWNED BY and check the after effects on the access privileges on the AN
$an->safe_psql('postgres', 'DROP OWNED BY regress_dep_user1');

for my $node ($an, $dn1, $dn2)
{
	$node->psql_is(
		'postgres', $query,
		q[regress_dep_user0=arwdDxt/regress_dep_user0],
		'Node shows correct set of reduced access privileges');
}

# Check the ownership on AN and DN post the REASSIGN
$query =
  q[SELECT pg_catalog.pg_get_userbyid(c.relowner) as owner FROM pg_catalog.pg_class c WHERE c.relname = 'conditions1';];

$an->safe_psql('postgres',
	'REASSIGN OWNED BY regress_dep_user0 TO regress_dep_user1;');

for my $node ($an, $dn1, $dn2)
{
	$node->psql_is('postgres', $query, q[regress_dep_user1],
		'Node shows changed object ownership');
}

done_testing();

1;

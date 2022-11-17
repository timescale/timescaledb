# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use AccessNode;
use DataNode;
use Test::More tests => 4;

#Initialize all the multi-node instances
my $an  = AccessNode->create('an');
my $dn1 = DataNode->create('dn1');
my $dn2 = DataNode->create('dn2');

$an->add_data_node($dn1);
$an->add_data_node($dn2);

$an->psql_is(
	'postgres',
	'SELECT * FROM _timescaledb_internal.health() ORDER BY 1 NULLS FIRST',
	q[|t|f|
dn1|t|f|
dn2|t|f|], 'Health check shows healthy AN and two healthy DNs');

# Stop a data node to simulate failure
$dn1->stop('fast');

# Health check will currently fail with an error when a data node
# cannot be contacted. This should be fixed so that the health check
# instead returns a negative status for the node.
my ($ret, $stdout, $stderr) = $an->psql('postgres',
	'SELECT * FROM _timescaledb_internal.health() ORDER BY 1 NULLS FIRST;');

# psql return error code 3 in case of failure in script
is($ret, qq(3), "expect error code 3 due to failed data node");

done_testing();

1;


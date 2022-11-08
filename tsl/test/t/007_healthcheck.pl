# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use AccessNode;
use DataNode;
use Test::More tests => 8;

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

my ($ret, $stdout, $stderr) = $an->psql('postgres',
	'SELECT * FROM _timescaledb_internal.health() ORDER BY 1 NULLS FIRST;');

is($ret, qq(0), "PSQL finishes statement without error");

# On data node failure, _timescaledb_internal.health() prints an arbitrary
# connection string in the error message. Instead of a parser, this is solved
# by a multiline regexp. If you are reading this, I'm sorry.
like(
	$stdout,
	qr/
		^\|t\|f\|\n
		^dn1\|f\|f\|.*connect(ion)?\sto\sserver.*
		^dn2\|t\|f\|$
		/smx,
	"Health check shows unhealthy DN with connection error next to healthy DN and AN"
);
$dn1->start();

# Test onlining of a DataNode
$an->psql_is(
	'postgres',
	'SELECT * FROM _timescaledb_internal.health() ORDER BY 1 NULLS FIRST',
	q[|t|f|
dn1|t|f|
dn2|t|f|], 'Health check shows healthy AN and two healthy DNs');

done_testing();

1;

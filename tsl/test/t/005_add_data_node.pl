# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use PostgresNode;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 1;

#
# Make sure AN does not crash and produce correct error in case if the
# DN is not properly configured.
#
# Issue:
# https://github.com/timescale/timescaledb/issues/3951

my $an = AccessNode->create('an');
my $dn = get_new_node('dn');
$dn->init();
$dn->start();

my $name = $dn->name;
my $host = $dn->host;
my $port = $dn->port;
my ($ret, $stdout, $stderr) =
  $an->psql('postgres',
	"SELECT add_data_node('$name', host => '$host', port => $port)");
like(
	$stderr,
	qr/extension "timescaledb" must be preloaded/,
	'failure when adding data node');

done_testing();

1;

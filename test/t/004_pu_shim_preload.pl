# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

# Hook preloaded ahead of the loader stays first when the versioned extension never loads.
my $node = PostgreSQL::Test::Cluster->new('pu_shim_preload');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'timescaledb_pu_probe,timescaledb'");
$node->start;

# No CREATE EXTENSION here, so the DROP exercises the shim predecessor fallback.
my ($rc, $out, $err) = $node->psql(
	'postgres', q{
CREATE TABLE shim_prev_test(i int);
DROP TABLE shim_prev_test;
});
is($rc, 0, 'DDL succeeds with probe preloaded ahead of loader')
  or diag("stdout: $out\nstderr: $err");
like(
	$err,
	qr/pu-probe got DROP TABLE 'shim_prev_test'/,
	'probe hook ran via shim predecessor fallback');

is( $node->safe_psql('postgres', 'SHOW shared_preload_libraries'),
	'timescaledb_pu_probe,timescaledb',
	'expected preload order');

$node->stop;
done_testing();

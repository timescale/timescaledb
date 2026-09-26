# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

# Old (pre-v6) loader must still load the current extension, just without
# hook-order control. The mock old loader is preloaded instead of the real one.
#
# Uses LOAD, not CREATE EXTENSION: the install script calls
# restart_background_workers() from the real loader, which the mock lacks.
my $node = PostgreSQL::Test::Cluster->new('old_loader');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'timescaledb_mock_old_loader'");
$node->start;

# LOAD and DDL in one session (LOAD is backend-local).
my ($rc, $out, $err) = $node->psql(
	'postgres', q{
LOAD '$libdir/timescaledb-@PROJECT_VERSION_MOD@';
CREATE TABLE compat_test(i int);
DROP TABLE compat_test;
});
is($rc, 0, 'LOAD and DDL with old loader succeed')
  or diag("stdout: $out\nstderr: $err");

# LOAD installs nothing.
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'"),
	'0',
	'nothing installed');

$node->stop;
done_testing();

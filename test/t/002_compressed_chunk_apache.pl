# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

# Regression test for issue #7787: if a user has compressed chunks
# produced by the Community/TSL edition and then switches the
# "timescaledb.license" GUC to "apache", a plain SELECT against the
# hypertable must NOT silently return the near-empty contents of the
# uncompressed chunk relation. It must raise a clear error pointing
# at the license as the cause.

use strict;
use warnings;
use TimescaleNode;
use Test::More tests => 3;

my $node = TimescaleNode->create('compressed_apache');

# Override the default apache license from the shared postgresql.conf
# template so we can actually create and compress a hypertable.
$node->append_conf('postgresql.conf', "timescaledb.license='timescale'");
$node->restart;

$node->safe_psql(
	'postgres', q[
	CREATE TABLE metrics(time timestamptz NOT NULL, value float) WITH (tsdb.hypertable);
	INSERT INTO metrics
	SELECT generate_series('2023-01-01'::timestamptz,
	                       '2023-01-03'::timestamptz,
	                       '1 hour'::interval), random();
	SELECT count(compress_chunk(c)) FROM show_chunks('metrics') c;
]);

my $row_count = $node->safe_psql('postgres', 'SELECT count(*) FROM metrics');
is($row_count, '49', 'all rows visible under timescale license');

# Switch to apache license and restart.
$node->append_conf('postgresql.conf', "timescaledb.license='apache'");
$node->restart;

my ($rc, $stdout, $stderr) =
  $node->psql('postgres', 'SELECT count(*) FROM metrics');
isnt($rc, 0, 'querying compressed hypertable under apache license errors');
like(
	$stderr,
	qr/querying compressed data is not supported/,
	'error message points at the license');

done_testing();

1;

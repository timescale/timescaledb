# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Test copy_chunk_data(): using logical replication to copy chunk data
# between data nodes
#

use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 18;

# initialize multi-node cluster
my $an  = AccessNode->create('an');
my $dn1 = DataNode->create('dn1', allows_streaming => 'logical');
my $dn2 = DataNode->create('dn2', allows_streaming => 'logical');

# add the data nodes from the access node
$an->add_data_node($dn1);
$an->add_data_node($dn2);

# create a distributed hypertable and insert a few rows
$an->safe_psql(
	'postgres', qq[
CREATE TABLE test(time timestamp NOT NULL, device int, temp float);
SELECT create_distributed_hypertable('test', 'time', 'device', 3);
INSERT INTO test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
]
);

# check that chunks are shown appropriately on all nodes of the multi-node setup
my $query = q[SELECT * from show_chunks('test');];

# data node 1 chunks
$dn1->psql_is(
	'postgres', $query, q[_timescaledb_internal._dist_hyper_1_1_chunk
_timescaledb_internal._dist_hyper_1_3_chunk
_timescaledb_internal._dist_hyper_1_4_chunk],
	'DN1 shows correct set of chunks');

# data node 2 chunks
$dn2->psql_is(
	'postgres', $query,
	"_timescaledb_internal._dist_hyper_1_2_chunk",
	'DN2 shows correct set of chunks');

# check rows consistency on data node 1
my $_dist_hyper_1_1_chunk = 58;
my $_dist_hyper_1_4_chunk = 2;

$dn1->psql_is(
	'postgres',
	"SELECT count(*) FROM _timescaledb_internal._dist_hyper_1_1_chunk",
	"$_dist_hyper_1_1_chunk",
	'DN1 shows correct number of rows for _dist_hyper_1_1_chunk');

$dn1->psql_is(
	'postgres',
	"SELECT count(*) FROM _timescaledb_internal._dist_hyper_1_4_chunk",
	"$_dist_hyper_1_4_chunk",
	'DN1 shows correct number of rows _dist_hyper_1_4_chunk');

# copy chunks {1,4} from data node 1 to data node 2
$an->safe_psql(
	'postgres', qq[
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_1_1_chunk'::regclass, 'dn2');
SELECT _timescaledb_internal.copy_chunk_data('_timescaledb_internal._dist_hyper_1_1_chunk'::regclass, 'dn1', 'dn2');

SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_1_4_chunk'::regclass, 'dn2');
SELECT _timescaledb_internal.copy_chunk_data('_timescaledb_internal._dist_hyper_1_4_chunk'::regclass, 'dn1', 'dn2');
]
);

# check that data node 2 now has chunks with the data data node 1 chunks
$dn2->psql_is(
	'postgres',
	"SELECT count(*) FROM _timescaledb_internal._dist_hyper_1_1_chunk",
	"$_dist_hyper_1_1_chunk",
	'DN2 shows correct number of rows _dist_hyper_1_1_chunk');

$dn2->psql_is(
	'postgres',
	"SELECT count(*) FROM _timescaledb_internal._dist_hyper_1_4_chunk",
	"$_dist_hyper_1_4_chunk",
	'DN2 shows correct number of rows _dist_hyper_1_4_chunk');

done_testing();

1;

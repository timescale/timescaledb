# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test a simple multi node cluster creation and basic operations
use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 9;

#Initialize all the multi-node instances
my $an  = AccessNode->create('an');
my $dn1 = DataNode->create('dn1');
my $dn2 = DataNode->create('dn2');

$an->add_data_node($dn1);
$an->add_data_node($dn2);

#Create a distributed hypertable and insert a few rows
$an->safe_psql(
	'postgres',
	qq[
    CREATE TABLE test(time timestamp NOT NULL, device int, temp float);
    SELECT create_distributed_hypertable('test', 'time', 'device', 3);
    INSERT INTO test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
    ]);

#Check that chunks are shown appropriately on all nodes of the multi-node setup
my $query = q[SELECT * from show_chunks('test');];

#Query Access node
$an->psql_is(
	'postgres', $query, q[_timescaledb_internal._dist_hyper_1_1_chunk
_timescaledb_internal._dist_hyper_1_2_chunk
_timescaledb_internal._dist_hyper_1_3_chunk
_timescaledb_internal._dist_hyper_1_4_chunk], 'AN shows correct set of chunks'
);

#Query datanode1
$dn1->psql_is(
	'postgres',
	$query,
	"_timescaledb_internal._dist_hyper_1_1_chunk\n_timescaledb_internal._dist_hyper_1_3_chunk\n_timescaledb_internal._dist_hyper_1_4_chunk",
	'DN1 shows correct set of chunks');

#Query datanode2
$dn2->psql_is(
	'postgres', $query,
	"_timescaledb_internal._dist_hyper_1_2_chunk",
	'DN2 shows correct set of chunks');

done_testing();

1;

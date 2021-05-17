# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test a simple multi node cluster creation and basic operations
use strict;
use warnings;
use TimescaleNode qw(get_new_ts_node);
use TestLib;
use Test::More tests => 9;

#Initialize all the multi-node instances
my @nodes = ();
foreach my $nodename ('an', 'dn1', 'dn2')
{
	my $node = get_new_ts_node($nodename);
	$node->init;
	$node->start;
	# set up the access node
	if ($node->name eq 'an')
	{
		$node->safe_psql('postgres', "CREATE DATABASE an");
		$node->safe_psql('an',       "CREATE EXTENSION timescaledb");
	}
	push @nodes, $node;
}

#Add the data nodes from the access node
foreach my $i (1 .. 2)
{
	my $host = $nodes[$i]->host();
	my $port = $nodes[$i]->port();

	$nodes[0]->safe_psql('an',
		"SELECT add_data_node('dn$i', database => 'dn$i', host => '$host', port => $port)"
	);
}

#Create a distributed hypertable and insert a few rows
$nodes[0]->safe_psql(
	'an',
	qq[
    CREATE TABLE test(time timestamp NOT NULL, device int, temp float);
    SELECT create_distributed_hypertable('test', 'time', 'device', 3);
    INSERT INTO test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
    ]);

#Check that chunks are shown appropriately on all nodes of the multi-node setup
my $query = q[SELECT * from show_chunks('test');];

#Query Access node
$nodes[0]->psql_is(
	'an', $query, q[_timescaledb_internal._dist_hyper_1_1_chunk
_timescaledb_internal._dist_hyper_1_2_chunk
_timescaledb_internal._dist_hyper_1_3_chunk
_timescaledb_internal._dist_hyper_1_4_chunk], 'AN shows correct set of chunks'
);

#Query datanode1
$nodes[1]->psql_is(
	'dn1',
	$query,
	"_timescaledb_internal._dist_hyper_1_1_chunk\n_timescaledb_internal._dist_hyper_1_3_chunk\n_timescaledb_internal._dist_hyper_1_4_chunk",
	'DN1 shows correct set of chunks');

#Query datanode2
$nodes[2]->psql_is(
	'dn2', $query,
	"_timescaledb_internal._dist_hyper_1_2_chunk",
	'DN2 shows correct set of chunks');

done_testing();

1;

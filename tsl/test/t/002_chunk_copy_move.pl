# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test the multi node chunk copy/move operation end-to-end
use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 272;

#Initialize all the multi-node instances
my $an  = AccessNode->create('an');
my $dn1 = DataNode->create('dn1', allows_streaming => 'logical');
my $dn2 = DataNode->create('dn2', allows_streaming => 'logical');

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
my $query        = q[SELECT * from show_chunks('test');];
my $operation_id = "ts_copy_1_1";

#Check chunk states before the move
check_pre_move_chunk_states();

#Setup the error injection function on the AN
my $extversion = $an->safe_psql('postgres',
	"SELECT extversion from pg_catalog.pg_extension WHERE extname = 'timescaledb'"
);
$an->safe_psql(
	'postgres',
	qq[
    CREATE OR REPLACE FUNCTION error_injection_on(TEXT) RETURNS VOID LANGUAGE C VOLATILE STRICT
    AS 'timescaledb-$extversion', 'ts_debug_point_enable';
    ]);

#Induce errors in various stages in the chunk move activity and ensure that the
#cleanup function restores things to the previous sane state

my @stages =
  qw(init create_empty_chunk create_publication create_replication_slot create_subscription sync_start sync drop_publication drop_subscription attach_chunk delete_chunk);

my ($stdout, $stderr, $ret);
my $curr_index = 1;
my $arrSize    = @stages;

while ($curr_index < $arrSize)
{
	#Enable the error at each stage
	#Call the move_chunk procedure which should error out now
	($ret, $stdout, $stderr) = $an->psql('postgres',
		"SELECT error_injection_on('$stages[$curr_index]'); CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'dn1', destination_node => 'dn2');"
	);
	is($ret, 3,
		"move_chunk fails as expected in stage '$stages[$curr_index]'");
	like(
		$stderr,
		qr/ERROR:  error injected at debug point '$stages[$curr_index]'/,
		'failure in expected stage');

	#The earlier debug error point gets released automatically since it's a session lock
	#Call the cleanup procedure to make things right
	$operation_id = "ts_copy_" . $curr_index . "_1";
	$an->safe_psql('postgres',
		"CALL timescaledb_experimental.cleanup_copy_chunk_operation(operation_id=>'$operation_id');"
	);

	#Check chunk state is as before the move
	check_pre_move_chunk_states();

	$curr_index++;
}

#Move chunk _timescaledb_internal._dist_hyper_1_1_chunk to DN2 from AN
$an->safe_psql('postgres',
	"CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'dn1', destination_node => 'dn2')"
);

#Query datanode1 after the above move
$dn1->psql_is(
	'postgres',
	$query,
	"_timescaledb_internal._dist_hyper_1_3_chunk\n_timescaledb_internal._dist_hyper_1_4_chunk",
	'DN1 shows correct set of chunks');

#Check contents on the chunk on DN2, after the move
$dn2->psql_is(
	'postgres',
	"SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk",
	qq[406],
	"DN2 has correct contents after the move in the chunk");

#Query datanode2
$dn2->psql_is(
	'postgres',
	$query,
	"_timescaledb_internal._dist_hyper_1_2_chunk\n_timescaledb_internal._dist_hyper_1_1_chunk",
	'DN2 shows correct set of chunks');

#Copy chunk _timescaledb_internal._dist_hyper_1_1_chunk to DN1 from DN2
$an->safe_psql('postgres',
	"CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'dn2', destination_node => 'dn1')"
);

#Query datanode1 after the above copy
$dn1->psql_is(
	'postgres',
	$query,
	"_timescaledb_internal._dist_hyper_1_3_chunk\n_timescaledb_internal._dist_hyper_1_4_chunk\n_timescaledb_internal._dist_hyper_1_1_chunk",
	'DN1 shows correct set of chunks after the copy');

#Check contents on the chunk on DN2, after the copy
$dn1->psql_is(
	'postgres',
	"SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk",
	qq[406],
	"DN1 has correct contents after the copy in the chunk");

#Check contents on the chunk on DN2, after the copy
$dn2->psql_is(
	'postgres',
	"SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk",
	qq[406],
	"DN2 has correct contents after the copy in the chunk");

#Query datanode2
$dn2->psql_is(
	'postgres',
	$query,
	"_timescaledb_internal._dist_hyper_1_2_chunk\n_timescaledb_internal._dist_hyper_1_1_chunk",
	'DN2 shows correct set of chunks after the copy');

done_testing();

#Check the following
#1) chunk is still on "dn1",
#2) there's no entry on "dn2",
#3) there are no left over replication slots and publications on "dn1",
#4) there is no subscription on "dn2"
sub check_pre_move_chunk_states
{
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

	#Check contents on the chunk on DN1
	$dn1->psql_is(
		'postgres',
		"SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk",
		qq[406],
		"DN1 has correct contents in the chunk");

	#Query datanode2
	$dn2->psql_is(
		'postgres', $query,
		"_timescaledb_internal._dist_hyper_1_2_chunk",
		'DN2 shows correct set of chunks');

	#Check that there is no replication slot on datanode1
	$dn1->psql_is(
		'postgres',
		"SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = '$operation_id'",
		"",
		'DN1 doesn\'t have left over replication slots');

	#Check that there is no publication on datanode1
	$dn1->psql_is(
		'postgres',
		"SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = '$operation_id'",
		"",
		'DN1 doesn\'t have left over publication');

	#Check that there is no subscription on datanode2
	$dn2->psql_is(
		'postgres',
		"SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = '$operation_id'",
		"",
		'DN2 doesn\'t have left over subscription');
}

1;

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test a simple multi node cluster creation and basic operations
use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 17;

#Initialize all the multi-node instances
my $an = AccessNode->create('an');

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

# Check that distributed tables in non-default schema and also containing user created types
# in another schema are created properly
$query = q[CREATE SCHEMA myschema];
$an->safe_psql('postgres', "$query");
$query =
  q[CREATE TYPE public.full_address AS (city VARCHAR(90), street VARCHAR(90))];
$an->safe_psql('postgres', "$query; CALL distributed_exec('$query');");

# Create a table in the myschema schema using this unqualified UDT. Should work
$an->safe_psql('postgres',
	"CREATE TABLE myschema.test (time timestamp, a full_address);");

# A distributed table creation followed by sample INSERT should succeed now
$an->safe_psql('postgres',
	"SELECT create_distributed_hypertable('myschema.test', 'time');");
$an->safe_psql('postgres',
	"INSERT INTO myschema.test VALUES ('2018-03-02 1:00', ('Kilimanjaro', 'Diamond St'));"
);
$an->psql_is(
	'postgres',
	"SELECT * from myschema.test",
	q[2018-03-02 01:00:00|(Kilimanjaro,"Diamond St")],
	'AN shows correct data with UDT from different schema');


# Test behavior of size utillities when a data node is not responding
$an->psql_is(
	'postgres', "SELECT * FROM hypertable_size('myschema.test')",
	q[81920],   'AN hypertable_size() returns correct size');

$dn1->stop('fast');
my ($cmdret, $stdout, $stderr) =
  $an->psql('postgres', "SELECT * FROM hypertable_size('myschema.test')");

# Check that hypertable_size() returns error when dn1 is down
is($cmdret, 3);
like(
	$stderr,
	qr/ERROR:  could not connect to "dn1"/,
	'failure when connecting to dn1');

done_testing();

1;

# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# test a multi node cluster with read only queries from access node
# primary and standby nodes
use strict;
use warnings;
use AccessNode;
use DataNode;
use TestLib;
use Test::More tests => 15;

#Initialize all the multi-node instances

my $an = AccessNode->get_new_node('an');
$an->init(
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);
$an->start;
$an->safe_psql('postgres', 'CREATE EXTENSION timescaledb');

my $backup_name = 'my_backup';

# Take backup
$an->backup($backup_name);

# Create streaming standby linking to master
my $an_standby = AccessNode->get_new_node('an_standby_1');
$an_standby->init_from_backup($an, $backup_name, has_streaming => 1);
$an_standby->start;

#Initialize and set up data nodes now
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

my $query = qq[
CREATE OR REPLACE FUNCTION read_write_function()
  RETURNS VOID
  LANGUAGE plpgsql AS
\$func\$
BEGIN
      CREATE TABLE t_rd_wr(
       id serial PRIMARY KEY,
       customerid int,
       daterecorded date,
       value double precision
      );
END
\$func\$;
];

# Create a function which does READ WRITE activity on the datanode
$an->safe_psql('postgres', "$query; CALL distributed_exec('$query');");

#Allow standby to catch up with the primary
$an->wait_for_catchup($an_standby, 'replay');

#Check that chunks are shown appropriately from the AN standby node
$query = q[SELECT * from show_chunks('test');];

#Query Access Standby node
$an_standby->psql_is(
	'postgres', $query, q[_timescaledb_internal._dist_hyper_1_1_chunk
_timescaledb_internal._dist_hyper_1_2_chunk
_timescaledb_internal._dist_hyper_1_3_chunk
_timescaledb_internal._dist_hyper_1_4_chunk],
	'AN Standby shows correct set of chunks');

#Check that SELECT queries work ok from the AN standby node
my $result = $an_standby->safe_psql('postgres', "SELECT count(*) FROM test");
is($result, qq(145), 'streamed content on AN standby');

# Check that only READ-only queries can run on AN standby node
my ($ret, $stdout, $stderr) =
  $an_standby->psql('postgres', 'INSERT INTO test(time) VALUES (now())');
is($ret, qq(3), "failed as expected");
like(
	$stderr,
	qr/cannot execute INSERT in a read-only transaction/,
	"read only message as expected");

# Check that queries which connect to datanodes also remain read only
($ret, $stdout, $stderr) =
  $an_standby->psql('postgres',
	'CALL distributed_exec($$ CREATE USER testrole WITH LOGIN $$)');
is($ret, qq(3), "failed as expected");
like(
	$stderr,
	qr/cannot execute CREATE ROLE in a read-only transaction/,
	"read only message for DNs as expected");

# Check that function doing read write activity doesn't work
($ret, $stdout, $stderr) =
  $an_standby->psql('postgres',
	'CALL distributed_exec($$ SELECT read_write_function() $$)');
is($ret, qq(3), "failed as expected");
like(
	$stderr,
	qr/cannot execute CREATE TABLE in a read-only transaction/,
	"read only message for DNs as expected");

$an->append_conf(
	'postgresql.conf', qq[
        client_min_messages = 'debug3'
    ]);
$an->restart;
# Check that AN primary uses 2PC for read write transactions when multiple DNs
# are involved
($ret, $stdout, $stderr) =
  $an->psql('postgres',
	'BEGIN TRANSACTION READ WRITE; SELECT count(*) FROM TEST; ROLLBACK;');
like(
	$stderr,
	qr/use 2PC: true/,
	"read write transaction uses 2PC with 2DNs on AN as expected");

# Check that AN primary uses 2PC for read write transactions even when SINGLE DN
# is involved
($ret, $stdout, $stderr) = $an->psql('postgres',
	'CALL distributed_exec($$ CREATE USER testrole WITH LOGIN $$, node_list => \'{ "dn1" }\'); ROLLBACK;'
);
like(
	$stderr,
	qr/use 2PC: true/,
	"read write transaction uses 2PC even with ONE DN from AN as expected");

# Check that AN primary uses 1PC for READ ONLY SERIALIZABLE transactions even when
# multiple DNs are involved
($ret, $stdout, $stderr) = $an->psql('postgres',
	'BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY; SELECT count(*) FROM TEST; ROLLBACK;'
);
like(
	$stderr,
	qr/use 2PC: false/,
	"read only serializable transaction uses 1PC on AN as expected");

# Check that AN primary uses 1PC for READ ONLY transactions even when multiple DNs
# are involved
($ret, $stdout, $stderr) =
  $an->psql('postgres',
	'BEGIN TRANSACTION READ ONLY; SELECT count(*) FROM TEST; ROLLBACK;');
like(
	$stderr,
	qr/use 2PC: false/,
	"read only transaction uses 1PC on AN as expected");

# Check that standby can do READ WRITE queries post promotion
$an_standby->promote;
$an_standby->safe_psql('postgres', 'INSERT INTO test(time) VALUES (now())');
$result = $an_standby->safe_psql('postgres', "SELECT count(*) FROM test");
is($result, qq(146), 'READ WRITE content on AN standby');

# Read write function should also work now
$an_standby->safe_psql('postgres',
	'CALL distributed_exec($$ SELECT read_write_function() $$)');

done_testing();

1;

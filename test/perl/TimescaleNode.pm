# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# This class extends PostgresNode with Timescale-specific
# routines for setup.

package TimescaleNode;
use parent ("PostgresNode");
use TestLib qw(slurp_file);
use strict;
use warnings;

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);
@EXPORT = qw(
  get_new_ts_node
);
@EXPORT_OK = qw(
);

#
# Get a new TS-enabled PostgreSQL instance
#
# It's not created yet, but ready to restore from backup,
# initdb, etc.
#
sub get_new_ts_node
{
	my ($name, $class) = @_;

    $class //= 'TimescaleNode';

	my $self = PostgresNode::get_new_node($name);
	$self = bless $self, $class;

	return $self;
}

# initialize the data directory and add TS specific parameters
sub init
{
	my ($self, %kwargs) = @_;

	$self->SUPER::init(%kwargs);
	# append into postgresql.conf from Timescale
	# template config file
	$self->append_conf('postgresql.conf', TestLib::slurp_file("$ENV{'BUILDIR'}/tsl/test/postgresql.conf"));
}

# helper function to check output from PSQL for a query
sub psql_is
{
	my ($self, $db, $query, $expected_stdout, $testname) = @_;
	my ($psql_rc, $psql_out, $psql_err) = $self->SUPER::psql($db, $query);
	PostgresNode::ok(!$psql_rc, "$testname: err_code check");
	PostgresNode::is($psql_err, '', "$testname: error_msg check");
	PostgresNode::is($psql_out, $expected_stdout,
		"$testname: psql output check");
}

1;

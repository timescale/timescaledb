# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# This class extends PostgresNode with Timescale-specific
# routines for setup.

package TimescaleNode;

# Using linebreaks here to prevent perltidy from performing vertical alignment.
# This functionality has changed in recent perltidy versions (e.g., 2021 10 29)
# and would restrict the versions of perltidy that can be used to format the
# sources.

use if $ENV{PG_VERSION_MAJOR} >= 15, 'parent', qw(PostgreSQL::Test::Cluster);

use if $ENV{PG_VERSION_MAJOR} < 15, 'parent', qw(PostgresNode);

use
  if $ENV{PG_VERSION_MAJOR} >= 15, 'PostgreSQL::Test::Utils', qw(slurp_file);

use if $ENV{PG_VERSION_MAJOR} < 15, 'TestLib', qw(slurp_file);

use strict;
use warnings;

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

sub create
{
	my ($class, $name, %kwargs) = @_;
	my $self =
	  ($ENV{PG_VERSION_MAJOR} >= 15)
	  ? $class->new($name)
	  : $class->get_new_node($name);
	$self->init(%kwargs);
	$self->start(%kwargs);
	$self->safe_psql('postgres', 'CREATE EXTENSION timescaledb');
	return $self;
}

# initialize the data directory and add TS specific parameters
sub init
{
	my ($self, %kwargs) = @_;

	$self->SUPER::init(%kwargs);
	# append into postgresql.conf from Timescale
	# template config file
	$self->append_conf('postgresql.conf',
		slurp_file("$ENV{'CONFDIR'}/postgresql.conf"));
	$self->append_conf('postgresql.conf', 'datestyle=ISO');
}

# helper function to check output from PSQL for a query
sub psql_is
{
	my ($self, $db, $query, $expected_stdout, $testname) = @_;
	my ($psql_rc, $psql_out, $psql_err) = $self->SUPER::psql($db, $query);
	if (($ENV{PG_VERSION_MAJOR} >= 15))
	{
		PostgreSQL::Test::Cluster::ok(!$psql_rc, "$testname: err_code check");
		PostgreSQL::Test::Cluster::is($psql_err, '',
			"$testname: error_msg check");
		PostgreSQL::Test::Cluster::is($psql_out, $expected_stdout,
			"$testname: psql output check");
	}
	else
	{
		PostgresNode::ok(!$psql_rc, "$testname: err_code check");
		PostgresNode::is($psql_err, '', "$testname: error_msg check");
		PostgresNode::is($psql_out, $expected_stdout,
			"$testname: psql output check");
	}
}

1;

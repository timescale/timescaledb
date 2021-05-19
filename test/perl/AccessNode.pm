# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

package AccessNode;
use parent qw(TimescaleNode);
use strict;
use warnings;

sub add_data_node
{
	my ($self, $dn) = @_;
	my $name = $dn->name;
	my $host = $dn->host;
	my $port = $dn->port;
	$self->safe_psql('postgres',
		"SELECT add_data_node('$name', host => '$host', port => $port)");
	return $self;
}

1;

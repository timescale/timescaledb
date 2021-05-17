# Perl-based TAP tests

`test/perl/` contains shared infrastructure that's used by Perl-based tests
across the source tree

The tests are invoked via perl's `prove` command. By default every
test in the t/ subdirectory is run. Individual test(s) can be run
instead by passing something like `PROVE_TESTS="t/001_testname.pl
t/002_othertestname.pl"` to make.

You should prefer to write tests using `pg_regress`, or
isolation tester specs, if possible.

Note that all tests and test tools should have perltidy run on them
using perltidy, for example:

```
perltidy --profile=$TS_SRC_DIR/scripts/perltidyrc /path/to/taptest
```

## Writing tests

Tests are written using Perl's `Test::More` with some PostgreSQL-specific
infrastructure from `src/test/perl` providing node management, support for
invoking `psql` to run queries and get results, etc. You should read the
documentation for `Test::More` before trying to write tests.

The PG specific infrastructure has been extended via the `TimescaleNode`
class in this directory to add timescale specific configuration parameters
and some often used helper functions.

Test scripts in the t/ subdirectory of a suite are executed in alphabetical
order.

Each test script should begin with:

```
    use strict;
    use warnings;
    use TimescaleNode;
    use TestLib;
    # Replace with the number of tests to execute:
    use Test::More tests => 1;
```

then it will generally need to set up one or more nodes, run commands
against them and evaluate the results. For example:

```
    my $node = get_new_ts_node('access_node');
    $node->init;
    $node->start;

    my $ret = $node->safe_psql('postgres', 'SELECT 1');
    is($ret, '1', 'SELECT 1 returns 1');

    $node->stop('fast');
```

`Test::More::like` entails use of the qr// operator.  Avoid Perl 5.8.8 bug
#39185 by not using the "$" regular expression metacharacter in qr// when also
using the "/m" modifier.  Instead of "$", use "\n" or "(?=\n|\z)".

Read the `Test::More` documentation for more on how to write tests:

```
    perldoc Test::More
```

For available PostgreSQL-specific test methods and some example tests read the
perldoc for the test modules, e.g.:

```
    cd $COMMUNITY_PG_SRCS
    perldoc src/test/perl/PostgresNode.pm
```

## Required Perl

Tests must run on perl `5.8.0` and newer. `perlbrew` is a good way to obtain such
a Perl; see http://perlbrew.pl .

Just install and

```
    perlbrew --force install 5.8.0
    perlbrew use 5.8.0
    perlbrew install-cpanm
    cpanm install IPC::Run
```

then re-run configure to ensure the correct Perl is used when running
tests. To verify that the right Perl was found:

```
    grep ^PERL= config.log
```

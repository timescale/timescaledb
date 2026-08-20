#!/usr/bin/env bash

# This oracle runs the reproducer script with timescaledb.enable_optimizations
# set to ON and OFF, and compares the output. The admissible bug must lead to
# a change in script output with optimizations disabled.
#
# An admissible repro script:
#
# Must be runnable on same database multiple times in sequence.
#
# Must run exactly the same statements no matter if it runs with optimizations
# enabled or disabled.
#
# Must not use the psql meta-commands.
#
# Must not require superuser privileges.
#
# The output of an admissible repro script:
#
# Must be sufficiently ordered to prevent false positives (i.e. ORDER BY, no
# ties).
#
# Must not depend on floating point precision or numeric stability.
#
# Must be independent from arbitrary environmental influence like the OID values
# or chunk identifiers.
#
# Must not change when the script runs on the same database multiple times in
# sequence.
set -eu

psql <<<'alter database :"DBNAME" set client_min_messages to error'
psql <<<'alter database :"DBNAME" set timescaledb.enable_optimizations to off'

if ! psql -f "$1" > result_noopt.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -c "set enable_seqscan to off;" -f "$1" > result_noopt_noseq.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -c "set enable_indexscan to off;" -f "$1" > result_noopt_noindex.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -c "set enable_hashagg to off;" -f "$1" > result_noopt_nohashagg.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -c "
        set max_parallel_workers_per_gather = 8;
        set parallel_setup_cost = 0;
        set parallel_tuple_cost = 0;
        set min_parallel_table_scan_size = 0;
        set min_parallel_index_scan_size = 0;
    " -f "$1" > result_noopt_para.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -c "set work_mem = '4GB'" -f "$1" > result_noopt_mem.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! diff -u result_noopt.txt result_noopt_noseq.txt \
    || ! diff -u result_noopt.txt result_noopt_noindex.txt \
    || ! diff -u result_noopt.txt result_noopt_nohashagg.txt \
    || ! diff -u result_noopt.txt result_noopt_para.txt \
    || ! diff -u result_noopt.txt result_noopt_mem.txt
then
    echo "Repro gives different results between runs, not admissible"
    exit 0
fi

psql <<<'alter database :"DBNAME" set timescaledb.enable_optimizations to on'

if ! psql -f "$1" > result_opt.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

echo
echo '```diff'
echo

if diff -u result_noopt.txt result_opt.txt
then
    result=0
else
    result=$?
fi

echo
echo '```'
echo

if [ "${result}" -eq 0 ]
then
    echo "Same result with optimizations ON/OFF, error not reproduced"
    exit 0
fi

echo "Reproduced"
exit 1

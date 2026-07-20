#!/usr/bin/env bash

# This oracle runs the reproducer script with the psql variable :hyper set
# to false and true, and compares the output. The script passes the variable
# into CREATE TABLE ... WITH (tsdb.hypertable = :hyper, ...), so the same
# statements run against a plain Postgres table and a TimescaleDB
# hypertable. The admissible bug must lead to a change in script output
# between the two.
#
# An admissible repro script:
#
# Must be runnable on same database multiple times in sequence.
#
# Must run exactly the same statements no matter if it runs against the
# plain table or the hypertable.
#
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

PGOPTIONS='-c client_min_messages=error'
export PGOPTIONS

psql -q <<<'alter database :"DBNAME" set client_min_messages to error'

if ! psql -q -v hyper=false -f "$1" > result_plain.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if psql -q -v hyper=maybe -f "$1" > result_probe.txt
then
    echo "Repro does not use :hyper for tsdb.hypertable, not admissible"
    exit 0
fi

if ! psql -q -v hyper=off -f "$1" > result_plain_synonym.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi


if ! psql -q -v hyper=true -f "$1" > result_hyper.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -v hyper=true -c "set enable_seqscan to off;" -f "$1" > result_hyper_noseq.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -v hyper=true -c "set enable_indexscan to off;" -f "$1" > result_hyper_noindex.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -v hyper=true -c "set enable_hashagg to off;" -f "$1" > result_hyper_nohashagg.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -v hyper=true -c "
        set max_parallel_workers_per_gather = 8;
        set parallel_setup_cost = 0;
        set parallel_tuple_cost = 0;
        set min_parallel_table_scan_size = 0;
        set min_parallel_index_scan_size = 0;
    " -f "$1" > result_hyper_para.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -v hyper=true -c "set work_mem = '4GB'" -f "$1" > result_hyper_mem.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -qXAt -v ON_ERROR_STOP=1 -v hyper=true -c "
        set enable_hashagg to off;
        set enable_sort to off;
        set timescaledb.enable_chunkwise_aggregation to off;
    " -f "$1" > result_hyper_rowsort.txt 2> result_hyper_rowsort.err
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! diff -u result_plain.txt result_synonym.txt \
    || ! diff -u result_hyper.txt result_hyper_noseq.txt \
    || ! diff -u result_hyper.txt result_hyper_noindex.txt \
    || ! diff -u result_hyper.txt result_hyper_nohashagg.txt \
    || ! diff -u result_hyper.txt result_hyper_para.txt \
    || ! diff -u result_hyper.txt result_hyper_mem.txt \
    || ! diff -u result_hyper.txt result_hyper_rowsort.txt
then
    echo "Repro gives different results between runs, not admissible"
    exit 0
fi

echo
echo '```diff'
echo

if diff -u result_plain.txt result_hyper.txt
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
    echo "Same result with hypertable false/true, error not reproduced"
    exit 0
fi

echo "Reproduced"
exit 1

#!/usr/bin/env bash

# This oracle runs the reproducer script with timescaledb.enable_optimizations
# set to ON and OFF, and compares the output. The admissible bug must lead to
# a change in script output with optimizations disabled.
#
# The script output must be sufficiently ordered to prevent false positives
# (i.e. ORDER BY, no ties).
#
# The irrelevant output from the preparatory stages like table creation should
# be hidden using the "\o" psql option or other means.
#
# The script output must be independent from arbitrary environmental influence
# like the OID values.
#
# The script must be runnable on same database multiple times in sequence.
#
# The script must run exactly the same statements no matter if it runs with
# optimizations enabled or disabled.
set -eu

printf '\\restrict %s\n' "${RANDOM}" | tee restricted-repro.sql
cat ~/llm-fuzzer-repro.sql >> restricted-repro.sql

psql -c "create database repro_off"
export PGDATABASE=repro_off
psql -c "create extension timescaledb"
psql -c "alter database repro_off set timescaledb.enable_optimizations to off"

if ! psql -q -f restricted-repro.sql > result_noopt.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -c "set enable_seqscan to off;" -f restricted-repro.sql > result_noopt_noseq.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -c "set enable_indexscan to off;" -f restricted-repro.sql > result_noopt_noindex.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! psql -q -c "set enable_hashagg to off;" -f restricted-repro.sql > result_noopt_nohashagg.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if ! diff -u result_noopt.txt result_noopt_noseq.txt \
    || ! diff -u result_noopt.txt result_noopt_noindex.txt \
    || ! diff -u result_noopt.txt result_noopt_nohashagg.txt
then
    echo "Repro gives different results between runs, not admissible"
    exit 0
fi

psql -c "create database repro_on"
export PGDATABASE=repro_on
psql -c "create extension timescaledb"
psql -c "alter database repro_on set timescaledb.enable_optimizations to on"

if ! psql -q -f restricted-repro.sql > result_opt.txt
then
    echo "Repro errors out, not admissible"
    exit 0
fi

if diff -u result_noopt.txt result_opt.txt
then
    echo "Same result with optimizations ON/OFF, error not reproduced"
    exit 0
fi

echo "Reproduced"
exit 1

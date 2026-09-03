#!/usr/bin/env bash

# This oracle runs the reproducer script with the psql variable
# :compression_command substituted by SQL that puts the hypertables into a
# compression state, and compares the output: nothing compressed (the
# baseline), everything compressed, and everything but the newest chunk
# compressed. The admissible bug must lead to a change in script output
# between the baseline and a compressed state.
#
# The script must contain this line, exactly, on a line of its own, once:
#
#   :compression_command
#
# It must come after the data has been inserted and the columnstore settings
# have been configured, and before the statements whose output is compared.
# The substituted SQL produces no output. It compresses the chunks of every
# hypertable with columnstore enabled, including the materialization
# hypertables of continuous aggregates. A hypertable created with CREATE
# TABLE ... WITH (tsdb.hypertable, ...) has columnstore enabled by default,
# one created with create_hypertable() does not. Use tsdb.columnstore = false
# to keep a table in the rowstore in all states. Set tsdb.segmentby and
# tsdb.orderby before the :compression_command line.
#
# The script must not call compress_chunk, decompress_chunk, recompress_chunk,
# convert_to_columnstore, convert_to_rowstore or the compression, columnstore
# or compaction policies, and must not set the parallelism GUCs
# (max_parallel_workers_per_gather, parallel_setup_cost, ...): the oracle owns
# the compression state and runs each compressed state with parallelism
# disabled and forced. A script that mentions any of these outside of --
# comments is rejected as inadmissible, as is a script that has nothing to
# compress at :compression_command.
#
# The data should span at least two chunks (pick a tsdb.chunk_interval smaller
# than the time span of the data), so that the mixed state has both
# columnstore and rowstore chunks, and should stay small (a few thousand rows
# over a handful of chunks): the substituted SQL compresses every chunk in one
# statement under the workflow's 30 second statement_timeout on an
# AddressSanitizer build.
#
# To exercise partially compressed chunks, insert a second batch of rows after
# the :compression_command line with timestamps inside an existing chunk. In
# the mixed state the newest chunk is uncompressed, so backfill an older one.
#
# Grouping by non-segmentby columns reaches the VectorAgg hash grouping paths,
# vary between one and several grouping columns. Built-in C functions
# (substring, length, ...) reach the vectorized function paths, SQL-language
# functions do not.
#
# An admissible repro script:
#
# Must be runnable on same database multiple times in sequence.
#
# Must run exactly the same statements in every state, using
# :compression_command only, and only once, to control the compression.
#
# Must not use the psql meta-commands.
#
# Must not require superuser privileges.
#
# The output of an admissible repro script:
#
# Must be sufficiently ordered to prevent false positives (i.e. ORDER BY, no
# ties), including an ORDER BY inside order-dependent aggregates such as
# string_agg and array_agg, because compression changes the physical row
# order.
#
# Must not depend on floating point precision or numeric stability.
#
# Must be independent from arbitrary environmental influence like the OID
# values or chunk identifiers, and must not query the compression state
# (e.g. timescaledb_information.chunks.is_compressed).
#
# Must not change when the script runs on the same database multiple times in
# sequence.
set -eu

repro=$1

# Two different no-ops, to reject a script whose output depends on the text of
# the variable.
noop_1='do $$ begin end $$;'
noop_2='do $$ begin null; end $$;'

probe='select 1/0;'

# No % and no quotes, these go through RAISE and single quotes.
nothing_to_compress='no chunk of a hypertable with columnstore enabled exists'
mixed_left_uncompressed='oracle defect: the mixed state left a hypertable with several time slices entirely uncompressed'

# Decompress first, in case the script keeps its tables between runs.
decompress_all=$(cat <<'SQL'
do $$ begin
    perform decompress_chunk(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)
    from timescaledb_information.chunks c
    where c.is_compressed;
end $$;
SQL
)

# The hypertables view excludes the materialization hypertables of caggs.
compressible=$(cat <<'SQL'
                select hypertable_schema, hypertable_name
                  from timescaledb_information.hypertables where compression_enabled
                union all
                select materialization_hypertable_schema, materialization_hypertable_name
                  from timescaledb_information.continuous_aggregates where compression_enabled
SQL
)

# Unquoted heredocs so that the variables expand, hence the escaped $$.
compress_all="${decompress_all}
$(cat <<SQL
do \$\$ begin
    perform compress_chunk(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)
    from timescaledb_information.chunks c
    where (c.hypertable_schema, c.hypertable_name) in (
${compressible})
      and not c.is_compressed;
    if not exists (
        select 1 from timescaledb_information.chunks c
        where (c.hypertable_schema, c.hypertable_name) in (
${compressible}))
    then
        raise exception '${nothing_to_compress}';
    end if;
end \$\$;
SQL
)"

# The chunks view fills range_end or range_end_integer depending on the
# dimension type, so the rank needs both or an integer-time hypertable ties at
# rank 1. The guard catches a rank that ties everything.
compress_all_but_newest="${decompress_all}
$(cat <<SQL
do \$\$ begin
    perform compress_chunk(format('%I.%I', s.chunk_schema, s.chunk_name)::regclass)
    from (select c.*,
                 dense_rank() over (partition by c.hypertable_schema, c.hypertable_name
                                    order by c.range_end desc, c.range_end_integer desc) as newest
          from timescaledb_information.chunks c
          where (c.hypertable_schema, c.hypertable_name) in (
${compressible})) s
    where s.newest > 1 and not s.is_compressed;
    if exists (
        select 1 from timescaledb_information.chunks c
        where (c.hypertable_schema, c.hypertable_name) in (
${compressible})
        group by c.hypertable_schema, c.hypertable_name
        having count(distinct coalesce(c.range_end::text, c.range_end_integer::text)) > 1
           and not bool_or(c.is_compressed))
    then
        raise exception '${mixed_left_uncompressed}';
    end if;
end \$\$;
SQL
)"

# Rowstore data in columnstore physical order.
compress_and_decompress_all="${compress_all}
${decompress_all}"

parallel_off='set max_parallel_workers_per_gather = 0;'
parallel_on='
    set max_parallel_workers_per_gather = 8;
    set parallel_setup_cost = 0;
    set parallel_tuple_cost = 0;
    set min_parallel_table_scan_size = 0;
    set min_parallel_index_scan_size = 0;'

if sed 's/--.*$//' "${repro}" | grep -n -i -E \
    '\b(compress_chunk|decompress_chunk|recompress_chunk|convert_to_columnstore|convert_to_rowstore|(add|remove)_(compression|columnstore|compaction)_policy)\b'
then
    echo "Repro manages the compression state itself instead of leaving it to :compression_command, not admissible"
    exit 0
fi

if sed 's/--.*$//' "${repro}" | grep -n -i -E \
    '\b(max_parallel_workers(_per_gather)?|parallel_(setup|tuple)_cost|min_parallel_(table|index)_scan_size|parallel_leader_participation|debug_parallel_query|force_parallel_mode)\b'
then
    echo "Repro sets the parallelism GUCs that the oracle controls, not admissible"
    exit 0
fi

psql <<<'alter database :"DBNAME" set client_min_messages to error'

run() {
    local name=$1
    shift
    if psql "$@" -f "${repro}" > "result_${name}.txt" 2> "result_${name}.err"
    then
        return 0
    fi
    cat "result_${name}.err" >&2
    if grep -q -F "${nothing_to_compress}" "result_${name}.err"
    then
        echo "Repro has nothing to compress at :compression_command (no hypertable with columnstore enabled has chunks by then), not admissible"
        exit 0
    fi
    if grep -q -F "${mixed_left_uncompressed}" "result_${name}.err"
    then
        echo "Oracle defect: the mixed state left a hypertable with several time slices entirely uncompressed, see verify.sh"
        exit 2
    fi
    echo "Repro errors out, not admissible"
    exit 0
}

run uncompressed -v compression_command="${noop_1}"

if psql -v compression_command="${probe}" -f "${repro}" &> result_probe.txt
then
    echo "Repro does not use :compression_command to control compression, not admissible"
    exit 0
fi

run uncompressed_repeat -v compression_command="${noop_2}"

run uncompressed_noseq -v compression_command="${noop_1}" -c "set enable_seqscan to off;"
run uncompressed_noindex -v compression_command="${noop_1}" -c "set enable_indexscan to off;"
run uncompressed_nohashagg -v compression_command="${noop_1}" -c "set enable_hashagg to off;"
run uncompressed_para -v compression_command="${noop_1}" -c "${parallel_on}"
run uncompressed_mem -v compression_command="${noop_1}" -c "set work_mem = '4GB'"

run roundtrip -v compression_command="${compress_and_decompress_all}"

run compressed -v compression_command="${compress_all}"
run compressed_paroff -v compression_command="${compress_all}" -c "${parallel_off}"
run compressed_paron -v compression_command="${compress_all}" -c "${parallel_on}"

run mixed -v compression_command="${compress_all_but_newest}"
run mixed_paroff -v compression_command="${compress_all_but_newest}" -c "${parallel_off}"
run mixed_paron -v compression_command="${compress_all_but_newest}" -c "${parallel_on}"

# Only the baseline is perturbed: a wrong compressed result is typically
# plan-dependent, and a changed plan would make it look like instability.
if ! diff -u result_uncompressed.txt result_uncompressed_repeat.txt \
    || ! diff -u result_uncompressed.txt result_uncompressed_noseq.txt \
    || ! diff -u result_uncompressed.txt result_uncompressed_noindex.txt \
    || ! diff -u result_uncompressed.txt result_uncompressed_nohashagg.txt \
    || ! diff -u result_uncompressed.txt result_uncompressed_para.txt \
    || ! diff -u result_uncompressed.txt result_uncompressed_mem.txt
then
    echo "Repro gives different results between runs, not admissible"
    exit 0
fi

# Order-only difference: under-specified ORDER BY. Content difference: the
# round trip changed the data, reported below.
if ! diff -q result_uncompressed.txt result_roundtrip.txt > /dev/null \
    && diff -q <(LC_ALL=C sort result_uncompressed.txt) <(LC_ALL=C sort result_roundtrip.txt) > /dev/null
then
    diff -u result_uncompressed.txt result_roundtrip.txt || true
    echo "Repro output depends on the physical row order (under-specified ORDER BY), not admissible"
    exit 0
fi

# Fixed labels, so that identical differences give identical diff files.
differing=""
roundtrip_differs=0
compressed_differs=0
mixed_differs=0
for variant in roundtrip compressed compressed_paroff compressed_paron \
    mixed mixed_paroff mixed_paron
do
    if ! diff -u -L "uncompressed baseline" -L "variant" \
            result_uncompressed.txt "result_${variant}.txt" > "diff_${variant}.txt"
    then
        differing="${differing} ${variant}"
        case "${variant}" in
            roundtrip) roundtrip_differs=1 ;;
            compressed*) compressed_differs=1 ;;
            mixed*) mixed_differs=1 ;;
        esac
    fi
done

if [ -z "${differing}" ]
then
    echo "Same result in all compression states, error not reproduced"
    exit 0
fi

# The job summary shows the first 100 lines: verdict and SQL first, diffs last.
echo "Reproduced: output differs from the uncompressed baseline in these variants:${differing}"

if [ "${roundtrip_differs}" -eq 1 ]
then
    echo
    echo "The roundtrip variant compresses every chunk and decompresses it again, so the data is"
    echo "back in the rowstore, yet the output differs from the baseline even after sorting the"
    echo "lines. Either the compress/decompress round trip changed the data, or an order-dependent"
    echo "aggregate in the script (string_agg, array_agg, ...) lacks an ORDER BY. It substitutes"
    echo "this for :compression_command:"
    echo
    echo '```sql'
    echo "${compress_and_decompress_all}"
    echo '```'
fi

if [ "${compressed_differs}" -eq 1 ]
then
    echo
    echo "The compressed variants substitute this for :compression_command:"
    echo
    echo '```sql'
    echo "${compress_all}"
    echo '```'
fi

if [ "${mixed_differs}" -eq 1 ]
then
    echo
    echo "The mixed variants substitute this for :compression_command:"
    echo
    echo '```sql'
    echo "${compress_all_but_newest}"
    echo '```'
fi

# Each distinct diff once, with the variants that produced it.
for variant in ${differing}
do
    group=""
    for other in ${differing}
    do
        if cmp -s "diff_${variant}.txt" "diff_${other}.txt"
        then
            group="${group} ${other}"
        fi
    done
    first=${group# }
    first=${first%% *}
    if [ "${first}" = "${variant}" ]
    then
        echo
        echo "Difference for${group}:"
        if diff -q <(LC_ALL=C sort result_uncompressed.txt) <(LC_ALL=C sort "result_${variant}.txt") > /dev/null
        then
            echo
            echo "These variants return the same lines as the baseline in a different order. The"
            echo "ordering of the script survived the planner knobs and the compress/decompress round"
            echo "trip on the rowstore, so this is most likely a genuine ordering bug of the"
            echo "columnstore, e.g. an ORDER BY that a compressed plan does not honor."
        fi
        echo
        echo '```diff'
        echo
        cat "diff_${variant}.txt"
        echo
        echo '```'
    fi
done

echo
echo "Reproduced"
exit 1

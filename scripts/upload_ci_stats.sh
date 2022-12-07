#!/usr/bin/env bash
set -xue

if ! [ -e 'installcheck.log' ]
then
    # Probably the previous steps have failed and we have nothing to upload.
    echo "installcheck.log does not exist"
    exit 0
fi

if [ -z "${CI_STATS_DB:-}" ]
then
    # The secret with the stats db connection string is not accessible in forks.
    echo "The statistics database connection string is not specified"
    exit 0
fi

PSQL=(psql "${CI_STATS_DB}" -qtAX "--set=ON_ERROR_STOP=1")

# The tables we are going to use.
DESIRED_SCHEMA="
create extension if not exists timescaledb;

create table job(
    job_date timestamptz, -- Serves as a unique id.
    commit_sha text,
    job_name text,
    repository text,
    ref_name text,
    event_name text,
    pr_number int,
    job_status text,
    url text,
    run_attempt int,
    run_id bigint,
    run_number int
);

create unique index on job(job_date);

select create_hypertable('job', 'job_date');

create table test(
    job_date timestamptz,
    test_name text,
    test_status text,
    test_duration float
);

create unique index on test(job_date, test_name);

select create_hypertable('test', 'job_date');

create table log(
    job_date timestamptz,
    test_name text,
    log_contents text
);

create unique index on log(job_date, test_name);

select create_hypertable('log', 'job_date');

-- don't add a trailing newline because bash command substitution removes it"

DROP_QUERY="
drop table if exists test cascade;
drop table if exists job cascade;
drop table if exists log cascade;
"

# Recreate the tables if the schema changed.
EXISTING_SCHEMA=$("${PSQL[@]}" -c "
    create table if not exists _schema(create_query text, drop_query text);
    select create_query from _schema;
")

if ! [ "${EXISTING_SCHEMA}" == "${DESIRED_SCHEMA}" ];
then
    "${PSQL[@]}" -v new_create="$DESIRED_SCHEMA" -v new_drop="$DROP_QUERY" <<<"
-- Run both the old and the new drop queries and ignore errors, to try to
-- bring the database into a predictable state even if it's current state is
-- incorrect (e.g. _schema doesn't actually match the existing tables).
\set ON_ERROR_STOP 0
select drop_query from _schema \gexec
:new_drop
\set ON_ERROR_STOP 1

-- Create new tables.
begin;
:new_create
truncate table _schema;
insert into _schema values (:'new_create', :'new_drop');
commit;
"
fi

# Create the job record.
COMMIT_SHA=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse @)
export COMMIT_SHA

JOB_NAME="${JOB_NAME:-test-job}"
export JOB_NAME

JOB_DATE=$("${PSQL[@]}" -c "
insert into job values (
    now(), '$COMMIT_SHA', '$JOB_NAME',
    '$GITHUB_REPOSITORY', '$GITHUB_REF_NAME', '$GITHUB_EVENT_NAME',
    '$GITHUB_PR_NUMBER', '$JOB_STATUS',
    'https://github.com/timescale/timescaledb/actions/runs/$GITHUB_RUN_ID/attempts/$GITHUB_RUN_ATTEMPT',
    '$GITHUB_RUN_ATTEMPT', '$GITHUB_RUN_ID', '$GITHUB_RUN_NUMBER')
returning job_date;
")
export JOB_DATE

# Split the regression.diffs into per-test files.
gawk '
    match($0, /^(diff|\+\+\+|\-\-\-) .*\/(.*)[.]out/, a) {
        file = a[2] ".diff";
        next;
    }

    { if (file) print $0 > file; }
' regression.log

# Snip the long sequences of "+" or "-" changes in the diffs.
for x in *.diff;
do
    if ! [ -e "$x" ] ; then continue ; fi
    gawk -v max_context_lines=10 -v min_context_lines=2 '
        /^-/ { new_sign = "-" }
        /^+/ { new_sign = "+" }
        /^[^+-]/ { new_sign = " " }

        {
            if (old_sign != new_sign) {
                to_print = lines_buffered > max_context_lines ? min_context_lines : lines_buffered;

                if (lines_buffered > to_print)
                    print "<" lines_buffered - to_print " lines skipped>";

                for (i = 0; i < to_print; i++) {
                    print buf[(NR + i - to_print) % max_context_lines]
                }

                printf("c %04d: %s\n", NR, $0);
                old_sign = new_sign;
                lines_printed = 0;
                lines_buffered = 0;
            } else {
                if (lines_printed >= min_context_lines) {
                    lines_buffered++;
                    buf[NR % max_context_lines] = sprintf("b %04d: %s", NR, $0)
                } else {
                    lines_printed++;
                    printf("p %04d: %s\n", NR, $0);
                }
            }
        }

        END {
            to_print = lines_buffered > max_context_lines ? min_context_lines : lines_buffered;

            if (lines_buffered > to_print)
                print "<" lines_buffered - to_print " lines skipped>";

            for (i = 0; i < to_print; i++) {
                print buf[(NR + 1 + i - to_print) % max_context_lines]
            }
        }' "$x" > "$x.tmp"
    mv "$x.tmp" "$x"
done

# Parse the installcheck.log to find the individual test results.
gawk -v OFS='\t' '
match($0, /^(test|    ) ([^ ]+)[ ]+\.\.\.[ ]+([^ ]+) (|\(.*\))[ ]+([0-9]+) ms$/, a) {
    print ENVIRON["JOB_DATE"], a[2], tolower(a[3] (a[4] ? (" " a[4]) : "")), a[5];
}
' installcheck.log > tests.tsv

# Save the test results into the database.
"${PSQL[@]}" -c "\copy test from tests.tsv"

# Upload the logs.
for x in sanitizer/* {sanitizer,stacktrace,postgres-failure}.log *.diff
do
    if ! [ -e "$x" ]; then continue ; fi
    "${PSQL[@]}" <<<"
        \set contents \`cat $x\`
        insert into log values ('$JOB_DATE', '$(basename "$x" .diff)', :'contents');
    "
done

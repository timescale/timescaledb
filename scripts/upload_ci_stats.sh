#!/usr/bin/env bash
set -xue

if [ -z "${CI_STATS_DB:-}" ]
then
    # The secret with the stats db connection string is not accessible in forks.
    echo "The statistics database connection string is not specified"
    exit 0
fi

PSQL=(psql "${CI_STATS_DB}" -qtAX "--set=ON_ERROR_STOP=1")

# The tables we are going to use. This schema is here just as a reminder, you'll
# have to create them manually. After you manually change the actual DB schema,
# don't forget to append the needed migration code below.
: "
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
"

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

# Parse the installcheck.log to find the individual test results. Note that this
# file might not exist for failed checks or non-regression checks like SQLSmith.
# We still want to save the other logs.
if [ -f 'installcheck.log' ]
then
    gawk -v OFS='\t' '
    match($0, /^(test|    ) ([^ ]+)[ ]+\.\.\.[ ]+([^ ]+) (|\(.*\))[ ]+([0-9]+) ms$/, a) {
        print ENVIRON["JOB_DATE"], a[2], tolower(a[3] (a[4] ? (" " a[4]) : "")), a[5];
    }
    ' installcheck.log > tests.tsv

    # Save the test results into the database.
    "${PSQL[@]}" -c "\copy test from tests.tsv"

    # Split the regression.diffs into per-test files.
    gawk '
        match($0, /^(diff|\+\+\+|\-\-\-) .*\/(.*)[.]out/, a) {
            file = a[2] ".diff";
            next;
        }

        { if (file) print $0 > file; }
    ' regression.log
fi

# Snip the long sequences of "+" or "-" changes in the diffs.
for x in *.diff;
do
    if ! [ -f "$x" ] ; then continue ; fi
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

# Upload the logs.
for x in sanitizer/* {sqlsmith/sqlsmith,sanitizer,stacktrace,postgres-failure}.log *.diff
do
    if ! [ -e "$x" ]; then continue ; fi
    "${PSQL[@]}" <<<"
        \set contents \`cat $x\`
        insert into log values ('$JOB_DATE', '$(basename "$x" .diff)', :'contents');
    "
done

# Background Worker Test Infrastructure

This directory contains mocks and hooks to enable testing of timescale
background workers. There are three main components: a counter-based timer used
to test the scheduler in a deterministic manner; a scheduler that inserts test
shims to the background worker and understands the tests we will run; and shims
to dynamically set time and intercept background worker output.

## Output

Background workers started by the test scheduler contain a hook storing all
`elog` and `ereport` output in the table

```SQL
public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT,
)
```

which must be created in order for tests to check background worker output.
`msg_no` contains which message this was in the total-order of all background
worker messages sent since the table was created; `mock_time` is the
virtualized timestamp ([see that section](## Timer)) at which the message was
written; `application_name` the name of the application that wrote the message;
`msg` is the messgage string itself.

See [`log.c`](log.c) for more detail.

(We want to print more data from `ErrorData` at a later date)

## Timer

We virtualize the timer to allow deterministic execution by tests. Our timer
store a virtual microsecond counter in shared memory, backround processes can
read this counter to determine the current time. The scheduler can "wait" on
this timer which optionally waits for a process to finish and updates the counter
to the waited time. The timer can be reset manually (for instance, to allow multiple
tests in one file) with `ts_bgw_params_reset_time`).

See [`timer_mock.c`](timer_mock.c) for more detail.

## Configuration

Settings and the timer for background worker tests are stored in shared memory.
This memory segment must be created with `ts_bgw_params_create` before use.

see [params.c](params.c) and [params.h](params.h) for more detail.

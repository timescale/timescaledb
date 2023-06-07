/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/lmgr.h>
#include <storage/proc.h>
#include <storage/shmem.h>
#include <utils/guc.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>
#include <utils/snapmgr.h>
#include <utils/memutils.h>
#include <utils/builtins.h>
#include <access/xact.h>
#include <pgstat.h>
#include <signal.h>

#include "extension.h"
#include "log.h"
#include "bgw/scheduler.h"
#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "timer_mock.h"
#include "params.h"
#include "test_utils.h"
#include "cross_module_fn.h"
#include "time_bucket.h"

TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_run);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_wait_for_scheduler_finish);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_test_main);
TS_FUNCTION_INFO_V1(ts_bgw_job_execute_test);
/* function for testing the correctness of the next_scheduled_slot calculation */
TS_FUNCTION_INFO_V1(ts_test_next_scheduled_execution_slot);

typedef enum TestJobType
{
	TEST_JOB_TYPE_JOB_1 = 0,
	TEST_JOB_TYPE_JOB_2_ERROR,
	TEST_JOB_TYPE_JOB_3_LONG,
	TEST_JOB_TYPE_JOB_4,
	_MAX_TEST_JOB_TYPE
} TestJobType;

static const char *test_job_type_names[_MAX_TEST_JOB_TYPE] = {
	[TEST_JOB_TYPE_JOB_1] = "bgw_test_job_1",
	[TEST_JOB_TYPE_JOB_2_ERROR] = "bgw_test_job_2_error",
	[TEST_JOB_TYPE_JOB_3_LONG] = "bgw_test_job_3_long",
	[TEST_JOB_TYPE_JOB_4] = "bgw_test_job_4",
};

/* this is copied from the job_stat/ts_get_next_scheduled_execution_slot */
extern Datum
ts_test_next_scheduled_execution_slot(PG_FUNCTION_ARGS)
{
	Interval *schedule_interval = PG_GETARG_INTERVAL_P(0);
	TimestampTz finish_time = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz initial_start = PG_GETARG_TIMESTAMPTZ(2);
	text *timezone = PG_ARGISNULL(3) ? NULL : PG_GETARG_TEXT_PP(3);

	Datum timebucket_fini, timebucket_init, result;
	Datum schedint_datum = IntervalPGetDatum(schedule_interval);
	Interval one_month = {
		.day = 0,
		.time = 0,
		.month = 1,
	};

	if (schedule_interval->month > 0)
	{
		if (timezone == NULL)
		{
			timebucket_init = DirectFunctionCall2(ts_timestamptz_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(initial_start));
			timebucket_fini = DirectFunctionCall2(ts_timestamptz_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time));
		}
		else
		{
			char *tz = text_to_cstring(timezone);
			timebucket_fini = DirectFunctionCall3(ts_timestamptz_timezone_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time),
												  CStringGetTextDatum(tz));

			timebucket_init = DirectFunctionCall3(ts_timestamptz_timezone_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(initial_start),
												  CStringGetTextDatum(tz));
		}
		/* always the next bucket */
		timebucket_fini =
			DirectFunctionCall2(timestamptz_pl_interval, timebucket_fini, schedint_datum);
		/* get the number of months between them */
		Datum year_init =
			DirectFunctionCall2(timestamptz_part, CStringGetTextDatum("year"), timebucket_init);
		Datum year_fini =
			DirectFunctionCall2(timestamptz_part, CStringGetTextDatum("year"), timebucket_fini);

		Datum month_init =
			DirectFunctionCall2(timestamptz_part, CStringGetTextDatum("month"), timebucket_init);
		Datum month_fini =
			DirectFunctionCall2(timestamptz_part, CStringGetTextDatum("month"), timebucket_fini);

		/* convert everything to months */
		float8 month_diff = DatumGetFloat8(year_fini) * 12 + DatumGetFloat8(month_fini) -
							(DatumGetFloat8(year_init) * 12 + DatumGetFloat8(month_init));

		Datum months_to_add = DirectFunctionCall2(interval_mul,
												  IntervalPGetDatum(&one_month),
												  Float8GetDatum(month_diff));
		result = DirectFunctionCall2(timestamptz_pl_interval,
									 TimestampTzGetDatum(initial_start),
									 months_to_add);
	}
	else
	{
		if (timezone == NULL)
		{
			/* it is safe to use the origin in time_bucket calculation */
			timebucket_fini = DirectFunctionCall3(ts_timestamptz_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time),
												  TimestampTzGetDatum(initial_start));
			result = timebucket_fini;
		}
		else
		{
			char *tz = text_to_cstring(timezone);
			timebucket_fini = DirectFunctionCall4(ts_timestamptz_timezone_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time),
												  CStringGetTextDatum(tz),
												  TimestampTzGetDatum(initial_start));
			result = timebucket_fini;
		}
	}
	while (DatumGetTimestampTz(result) <= finish_time)
	{
		result = DirectFunctionCall2(timestamptz_pl_interval, result, schedint_datum);
	}
	return result;
}

extern Datum
ts_bgw_db_scheduler_test_main(PG_FUNCTION_ARGS)
{
	Oid db_oid = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	BgwParams bgw_params;

	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */
	ts_bgw_scheduler_register_signal_handlers();
	BackgroundWorkerUnblockSignals();
	ts_bgw_scheduler_setup_callbacks();

	memcpy(&bgw_params, MyBgworkerEntry->bgw_extra, sizeof(bgw_params));

	elog(NOTICE, "scheduler user id %u", bgw_params.user_oid);
	elog(NOTICE, "running a test in the background: db=%u ttl=%d", db_oid, bgw_params.ttl);

	BackgroundWorkerInitializeConnectionByOid(db_oid, bgw_params.user_oid, 0);

	StartTransactionCommand();
	ts_params_get();
	ts_initialize_timer_latch();
	CommitTransactionCommand();

	ts_bgw_log_set_application_name("DB Scheduler");
	ts_register_emit_log_hook();

	ts_timer_set(&ts_mock_timer);

	ts_bgw_job_set_job_entrypoint_function_name("ts_bgw_job_execute_test");

	pgstat_report_appname("DB Scheduler Test");

	ts_bgw_scheduler_setup_mctx();

	ts_bgw_scheduler_process(bgw_params.ttl, ts_timer_mock_register_bgw_handle);

	PG_RETURN_VOID();
}

static BackgroundWorkerHandle *
start_test_scheduler(int32 ttl, Oid user_oid)
{
	const BgwParams bgw_params = {
		.bgw_main = "ts_bgw_db_scheduler_test_main",
		.ttl = ttl,
		.user_oid = user_oid,
	};

	/*
	 * This is where we would increment the number of bgw used, if we
	 * decide to do so
	 */
	ts_bgw_scheduler_setup_mctx();

	return ts_bgw_start_worker("ts_bgw_db_scheduler_test_main", &bgw_params);
}

/* this function will start up a bgw for the scheduler and set the ttl to the given value
 * (microseconds) */
extern Datum
ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(PG_FUNCTION_ARGS)
{
	BackgroundWorkerHandle *worker_handle;
	pid_t pid;

	worker_handle = start_test_scheduler(PG_GETARG_INT32(0), GetUserId());

	if (worker_handle != NULL)
	{
		BgwHandleStatus status = WaitForBackgroundWorkerStartup(worker_handle, &pid);
		TestAssertTrue(BGWH_STARTED == status);
		if (status != BGWH_STARTED)
			elog(ERROR, "bgw not started");

		status = WaitForBackgroundWorkerShutdown(worker_handle);
		TestAssertTrue(BGWH_STOPPED == status);
		if (status != BGWH_STOPPED)
			elog(ERROR, "bgw not stopped");
	}

	PG_RETURN_VOID();
}

static BackgroundWorkerHandle *current_handle = NULL;

extern Datum
ts_bgw_db_scheduler_test_run(PG_FUNCTION_ARGS)
{
	pid_t pid;
	MemoryContext old_ctx;
	BgwHandleStatus status;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	current_handle = start_test_scheduler(PG_GETARG_INT32(0), GetUserId());
	MemoryContextSwitchTo(old_ctx);

	status = WaitForBackgroundWorkerStartup(current_handle, &pid);
	TestAssertTrue(BGWH_STARTED == status);
	if (status != BGWH_STARTED)
		elog(ERROR, "bgw not started");

	PG_RETURN_VOID();
}

extern Datum
ts_bgw_db_scheduler_test_wait_for_scheduler_finish(PG_FUNCTION_ARGS)
{
	if (current_handle != NULL)
	{
		BgwHandleStatus status = WaitForBackgroundWorkerShutdown(current_handle);
		TestAssertTrue(BGWH_STOPPED == status);
		if (status != BGWH_STOPPED)
			elog(ERROR, "bgw not stopped");
	}
	PG_RETURN_VOID();
}

static bool
test_job_1()
{
	StartTransactionCommand();
	elog(WARNING, "Execute job 1");

	CommitTransactionCommand();
	return true;
}

static bool
test_job_2_error()
{
	StartTransactionCommand();
	elog(WARNING, "Before error job 2");

	elog(ERROR, "Error job 2");

	elog(WARNING, "After error job 2");

	CommitTransactionCommand();
	return true;
}

static pqsigfunc prev_signal_func = NULL;

static void
log_terminate_signal(SIGNAL_ARGS)
{
	write_stderr("job got term signal\n");
	if (prev_signal_func != NULL)
		prev_signal_func(postgres_signal_arg);
}

TS_FUNCTION_INFO_V1(ts_bgw_test_job_sleep);

/*
 * This function is used for testing removing jobs with
 * a currently running background job.
 */
Datum
ts_bgw_test_job_sleep(PG_FUNCTION_ARGS)
{
	BackgroundWorkerBlockSignals();

	/*
	 * Only set prev_signal_func once to prevent it from being set to
	 * log_terminate_signal.
	 */
	if (prev_signal_func == NULL)
		prev_signal_func = pqsignal(SIGTERM, log_terminate_signal);
	/* Setup any signal handlers here */
	BackgroundWorkerUnblockSignals();

	elog(WARNING, "Before sleep");
	PopActiveSnapshot();
	/*
	 * we commit here so the effect of the elog which is written
	 * to a table with a emit_log_hook is seen by other transactions
	 * to verify the background job started
	 */
	CommitTransactionCommand();

	StartTransactionCommand();
	DirectFunctionCall1(pg_sleep, Float8GetDatum(10));

	elog(WARNING, "After sleep");

	PG_RETURN_VOID();
}

static bool
test_job_3_long()
{
	BackgroundWorkerBlockSignals();

	/*
	 * Only set prev_signal_func once to prevent it from being set to
	 * log_terminate_signal.
	 */
	if (prev_signal_func == NULL)
		prev_signal_func = pqsignal(SIGTERM, log_terminate_signal);
	/* Setup any signal handlers here */
	BackgroundWorkerUnblockSignals();

	elog(WARNING, "Before sleep job 3");

	DirectFunctionCall1(pg_sleep, Float8GetDatum(0.5L));

	elog(WARNING, "After sleep job 3");
	return true;
}

/* Exactly like job 1, except a wrapper will change its next_start. */
static bool
test_job_4(void)
{
	elog(WARNING, "Execute job 4");
	return true;
}

static TestJobType
get_test_job_type_from_name(Name job_type_name)
{
	int i;

	for (i = 0; i < _MAX_TEST_JOB_TYPE; i++)
	{
		if (namestrcmp(job_type_name, test_job_type_names[i]) == 0)
			return i;
	}
	return _MAX_TEST_JOB_TYPE;
}

static bool
test_job_dispatcher(BgwJob *job)
{
	ts_register_emit_log_hook();
	ts_bgw_log_set_application_name(strdup(NameStr(job->fd.application_name)));

	StartTransactionCommand();
	ts_params_get();
	CommitTransactionCommand();

	switch (get_test_job_type_from_name(&job->fd.proc_name))
	{
		case TEST_JOB_TYPE_JOB_1:
			return test_job_1();
		case TEST_JOB_TYPE_JOB_2_ERROR:
			return test_job_2_error();
		case TEST_JOB_TYPE_JOB_3_LONG:
			return test_job_3_long();
		case TEST_JOB_TYPE_JOB_4:
		{
			/* Set next_start to 200ms */
			Interval new_interval = { .time = .2 * USECS_PER_SEC };
			return ts_bgw_job_run_and_set_next_start(job,
													 test_job_4,
													 3,
													 &new_interval,
													 /* atomic */ true,
													 /* mark */ false);
		}
		default:
			return ts_cm_functions->job_execute(job);
	}
	return false;
}

Datum
ts_bgw_job_execute_test(PG_FUNCTION_ARGS)
{
	ts_timer_set(&ts_mock_timer);
	ts_bgw_job_set_scheduler_test_hook(test_job_dispatcher);

	return ts_bgw_job_entrypoint(fcinfo);
}

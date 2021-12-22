/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
/*
 * This is a scheduler that takes background jobs and schedules them appropriately
 *
 * Limitations: For now the jobs are only loaded when the scheduler starts and are not
 * updated if the jobs table changes
 *
 */
#include <postgres.h>

#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>
#include <utils/acl.h>
#include <utils/inval.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>
#include <utils/snapmgr.h>
#include <utils/memutils.h>
#include <access/xact.h>
#include <pgstat.h>
#include <tcop/tcopprot.h>
#include <nodes/pg_list.h>

#include "compat/compat.h"
#include "extension.h"
#include "guc.h"
#include "job.h"
#include "job_stat.h"
#include "launcher_interface.h"
#include "scheduler.h"
#include "timer.h"
#include "version.h"

#define SCHEDULER_APPNAME "TimescaleDB Background Worker Scheduler"
#define START_RETRY_MS (1 * INT64CONST(1000)) /* 1 seconds */

static TimestampTz
least_timestamp(TimestampTz left, TimestampTz right)
{
	return (left < right ? left : right);
}

TS_FUNCTION_INFO_V1(ts_bgw_scheduler_main);

/*
 * Global so the invalidate cache message can set. Don't need to protect
 * access with a lock because it's accessed only by the scheduler process.
 */
static bool jobs_list_needs_update;

/* has to be global to shutdown jobs on exit */
static List *scheduled_jobs = NIL;

static MemoryContext scheduler_mctx;
static MemoryContext scratch_mctx;

/* See the README for a state transition diagram */
typedef enum JobState
{
	/* terminal state for now. Later we may have path to JOB_STATE_SCHEDULED */
	JOB_STATE_DISABLED,

	/*
	 * This is the initial state. next states: JOB_STATE_STARTED,
	 * JOB_STATE_DISABLED. This job is not running and has been scheduled to
	 * be started at a later time.
	 */
	JOB_STATE_SCHEDULED,

	/*
	 * next states: JOB_STATE_TERMINATING, JOB_STATE_SCHEDULED. This job has
	 * been started by the scheduler and is either running or finished (and
	 * the finish has not yet been detected by the scheduler).
	 */
	JOB_STATE_STARTED,

	/*
	 * next states: JOB_STATE_SCHEDULED. The scheduler has explicitly sent a
	 * terminate to this job but has not yet detected that it has stopped.
	 */
	JOB_STATE_TERMINATING
} JobState;

typedef struct ScheduledBgwJob
{
	BgwJob job;
	TimestampTz next_start;
	TimestampTz timeout_at;
	JobState state;
	BackgroundWorkerHandle *handle;

	bool reserved_worker;

	/*
	 * We say "may" here since under normal circumstances the job itself will
	 * perform the mark_end
	 */
	bool may_need_mark_end;
} ScheduledBgwJob;

static void on_failure_to_start_job(ScheduledBgwJob *sjob);

static volatile sig_atomic_t got_SIGHUP = false;

BackgroundWorkerHandle *
ts_bgw_start_worker(const char *function, const char *name, const char *extra)
{
	BackgroundWorker worker = {
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
		.bgw_start_time = BgWorkerStart_RecoveryFinished,
		.bgw_restart_time = BGW_NEVER_RESTART,
		.bgw_notify_pid = MyProcPid,
		.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId),
	};
	BackgroundWorkerHandle *handle = NULL;

	strlcpy(worker.bgw_name, name, BGW_MAXLEN);
	strlcpy(worker.bgw_library_name, ts_extension_get_so_name(), BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, function, BGW_MAXLEN);

	Assert(strlen(extra) < BGW_EXTRALEN);
	strlcpy(worker.bgw_extra, extra, BGW_EXTRALEN);

	/* handle needs to be allocated in long-lived memory context */
	MemoryContextSwitchTo(scheduler_mctx);
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		handle = NULL;
	MemoryContextSwitchTo(scratch_mctx);

	return handle;
}

#ifdef USE_ASSERT_CHECKING
static void
assert_that_worker_has_stopped(ScheduledBgwJob *sjob)
{
	pid_t pid;
	BgwHandleStatus status;

	Assert(sjob->reserved_worker);
	status = GetBackgroundWorkerPid(sjob->handle, &pid);
	Assert(BGWH_STOPPED == status);
}
#endif

static void
mark_job_as_started(ScheduledBgwJob *sjob)
{
	Assert(!sjob->may_need_mark_end);
	ts_bgw_job_stat_mark_start(sjob->job.fd.id);
	sjob->may_need_mark_end = true;
}

static void
mark_job_as_ended(ScheduledBgwJob *sjob, JobResult res)
{
	Assert(sjob->may_need_mark_end);
	ts_bgw_job_stat_mark_end(&sjob->job, res);
	sjob->may_need_mark_end = false;
}

static void
worker_state_cleanup(ScheduledBgwJob *sjob)
{
	/*
	 * This function needs to be safe wrt failures occurring at any point in
	 * the job starting process.
	 */
	if (sjob->handle != NULL)
	{
#ifdef USE_ASSERT_CHECKING
		/* Sanity check: worker has stopped (if it was started) */
		assert_that_worker_has_stopped(sjob);
#endif
		pfree(sjob->handle);
		sjob->handle = NULL;
	}

	/*
	 * first cleanup reserved workers before accessing db. Want to minimize
	 * the possibility of errors before worker is released
	 */
	if (sjob->reserved_worker)
	{
		ts_bgw_worker_release();
		sjob->reserved_worker = false;
	}

	if (sjob->may_need_mark_end)
	{
		BgwJobStat *job_stat;

		if (!ts_bgw_job_get_share_lock(sjob->job.fd.id, CurrentMemoryContext))
		{
			elog(WARNING,
				 "scheduler detected that job %d was deleted after job quit",
				 sjob->job.fd.id);
			ts_bgw_job_cache_invalidate_callback();
			sjob->may_need_mark_end = false;
			return;
		}

		job_stat = ts_bgw_job_stat_find(sjob->job.fd.id);

		Assert(job_stat != NULL);

		if (!ts_bgw_job_stat_end_was_marked(job_stat))
		{
			/*
			 * Usually the job process will mark the end, but if the job gets
			 * a signal (cancel or terminate), it won't be able to so we
			 * should.
			 */
			elog(LOG, "job %d failed", sjob->job.fd.id);
			mark_job_as_ended(sjob, JOB_FAILURE);
			/* reload updated value */
			job_stat = ts_bgw_job_stat_find(sjob->job.fd.id);
		}
		else
		{
			sjob->may_need_mark_end = false;
		}
	}
}

/* Set the state of the job.
 * This function is responsible for setting all of the variables in ScheduledBgwJob
 * except for the job itself.
 */
static void
scheduled_bgw_job_transition_state_to(ScheduledBgwJob *sjob, JobState new_state)
{
#ifdef USE_ASSERT_CHECKING
	JobState prev_state = sjob->state;
#endif

	BgwJobStat *job_stat;
	Oid owner_uid;

	switch (new_state)
	{
		case JOB_STATE_DISABLED:
			Assert(prev_state == JOB_STATE_STARTED || prev_state == JOB_STATE_TERMINATING);
			sjob->handle = NULL;
			break;
		case JOB_STATE_SCHEDULED:
			/* prev_state can be any value, including itself */

			worker_state_cleanup(sjob);

			job_stat = ts_bgw_job_stat_find(sjob->job.fd.id);

			Assert(!sjob->reserved_worker);
			sjob->next_start = ts_bgw_job_stat_next_start(job_stat, &sjob->job);
			break;
		case JOB_STATE_STARTED:
			Assert(prev_state == JOB_STATE_SCHEDULED);
			Assert(sjob->handle == NULL);
			Assert(!sjob->reserved_worker);

			StartTransactionCommand();

			if (!ts_bgw_job_get_share_lock(sjob->job.fd.id, CurrentMemoryContext))
			{
				elog(WARNING,
					 "scheduler detected that job %d was deleted when starting job",
					 sjob->job.fd.id);
				ts_bgw_job_cache_invalidate_callback();
				CommitTransactionCommand();
				MemoryContextSwitchTo(scratch_mctx);
				return;
			}

			/* If we are unable to reserve a worker go back to the scheduled state */
			sjob->reserved_worker = ts_bgw_worker_reserve();
			if (!sjob->reserved_worker)
			{
				elog(WARNING,
					 "failed to launch job %d \"%s\": out of background workers",
					 sjob->job.fd.id,
					 NameStr(sjob->job.fd.application_name));
				scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
				CommitTransactionCommand();
				MemoryContextSwitchTo(scratch_mctx);
				return;
			}

			/*
			 * start the job before you can encounter any errors so that they
			 * are always registered
			 */
			mark_job_as_started(sjob);
			if (ts_bgw_job_has_timeout(&sjob->job))
				sjob->timeout_at =
					ts_bgw_job_timeout_at(&sjob->job, ts_timer_get_current_timestamp());
			else
				sjob->timeout_at = DT_NOEND;

			owner_uid = get_role_oid(NameStr(sjob->job.fd.owner), false);
			CommitTransactionCommand();
			MemoryContextSwitchTo(scratch_mctx);

			elog(DEBUG1,
				 "launching job %d \"%s\"",
				 sjob->job.fd.id,
				 NameStr(sjob->job.fd.application_name));

			sjob->handle = ts_bgw_job_start(&sjob->job, owner_uid);
			if (sjob->handle == NULL)
			{
				elog(WARNING,
					 "failed to launch job %d \"%s\": failed to start a background worker",
					 sjob->job.fd.id,
					 NameStr(sjob->job.fd.application_name));
				on_failure_to_start_job(sjob);
				return;
			}
			Assert(sjob->reserved_worker);
			break;
		case JOB_STATE_TERMINATING:
			Assert(prev_state == JOB_STATE_STARTED);
			Assert(sjob->handle != NULL);
			Assert(sjob->reserved_worker);
			TerminateBackgroundWorker(sjob->handle);
			break;
	}
	sjob->state = new_state;
}

static void
on_failure_to_start_job(ScheduledBgwJob *sjob)
{
	StartTransactionCommand();
	if (!ts_bgw_job_get_share_lock(sjob->job.fd.id, CurrentMemoryContext))
	{
		elog(WARNING,
			 "scheduler detected that job %d was deleted while failing to start",
			 sjob->job.fd.id);
		ts_bgw_job_cache_invalidate_callback();
	}
	else
	{
		/* restore the original next_start to maintain priority (it is unset during mark_start) */
		if (sjob->next_start != DT_NOBEGIN)
			ts_bgw_job_stat_set_next_start(sjob->job.fd.id, sjob->next_start);
		mark_job_as_ended(sjob, JOB_FAILURE_TO_START);
	}
	scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
	CommitTransactionCommand();
	MemoryContextSwitchTo(scratch_mctx);
}

static inline void
bgw_scheduler_on_postmaster_death(void)
{
	/*
	 * Don't call exit hooks cause we want to bail out quickly. We don't care
	 * about cleaning up shared memory in this case anyway since it's
	 * potentially corrupt.
	 */
	on_exit_reset();
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while TimescaleDB scheduler was working")));
}

/*
 * This function starts a job.
 * To correctly count crashes we need to mark the start of a job in a separate
 * txn before we kick off the actual job. Thus this function cannot be run
 * from within a transaction.
 */
static void
scheduled_ts_bgw_job_start(ScheduledBgwJob *sjob,
						   register_background_worker_callback_type bgw_register)
{
	pid_t pid;
	BgwHandleStatus status;

	scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_STARTED);

	if (sjob->state != JOB_STATE_STARTED)
		return;

	Assert(sjob->handle != NULL);
	if (bgw_register != NULL)
		bgw_register(sjob->handle);

	status = WaitForBackgroundWorkerStartup(sjob->handle, &pid);
	switch (status)
	{
		case BGWH_POSTMASTER_DIED:
			bgw_scheduler_on_postmaster_death();
			break;
		case BGWH_STARTED:
			/* all good */
			break;
		case BGWH_STOPPED:
			StartTransactionCommand();
			scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
			CommitTransactionCommand();
			MemoryContextSwitchTo(scratch_mctx);
			break;
		case BGWH_NOT_YET_STARTED:
			/* should not be possible */
			elog(ERROR, "unexpected bgworker state %d", status);
			break;
	}
}

static void
terminate_and_cleanup_job(ScheduledBgwJob *sjob)
{
	if (sjob->handle != NULL)
	{
		TerminateBackgroundWorker(sjob->handle);
		WaitForBackgroundWorkerShutdown(sjob->handle);
	}
	sjob->may_need_mark_end = false;
	worker_state_cleanup(sjob);
}

/*
 *  Update the given job list with whatever is in the bgw_job table. For overlapping jobs,
 *  copy over any existing scheduler info from the given jobs list.
 *  Assume that both lists are ordered by job ID.
 *  Note that this function call will destroy cur_jobs_list and return a new list.
 */
List *
ts_update_scheduled_jobs_list(List *cur_jobs_list, MemoryContext mctx)
{
	List *new_jobs = ts_bgw_job_get_scheduled(sizeof(ScheduledBgwJob), mctx);
	ListCell *new_ptr = list_head(new_jobs);
	ListCell *cur_ptr = list_head(cur_jobs_list);

	while (cur_ptr != NULL && new_ptr != NULL)
	{
		ScheduledBgwJob *new_sjob = lfirst(new_ptr);
		ScheduledBgwJob *cur_sjob = lfirst(cur_ptr);

		if (cur_sjob->job.fd.id < new_sjob->job.fd.id)
		{
			/*
			 * We don't need cur_sjob anymore. Make sure to clean up the job
			 * state. Then keep advancing cur pointer until we catch up.
			 */
			terminate_and_cleanup_job(cur_sjob);

			cur_ptr = lnext_compat(cur_jobs_list, cur_ptr);
			continue;
		}
		if (cur_sjob->job.fd.id == new_sjob->job.fd.id)
		{
			/*
			 * Then this job already exists. Copy over any state and advance
			 * both pointers.
			 */
			cur_sjob->job = new_sjob->job;
			*new_sjob = *cur_sjob;

			/* reload the scheduling information from the job_stats */
			if (cur_sjob->state == JOB_STATE_SCHEDULED)
				scheduled_bgw_job_transition_state_to(new_sjob, JOB_STATE_SCHEDULED);

			cur_ptr = lnext_compat(cur_jobs_list, cur_ptr);
			new_ptr = lnext_compat(new_jobs, new_ptr);
		}
		else if (cur_sjob->job.fd.id > new_sjob->job.fd.id)
		{
			scheduled_bgw_job_transition_state_to(new_sjob, JOB_STATE_SCHEDULED);

			/* Advance the new_job list until we catch up to cur_list */
			new_ptr = lnext_compat(new_jobs, new_ptr);
		}
	}

	/* If there's more stuff in cur_list, clean it all up */
	if (cur_ptr != NULL)
	{
		ListCell *ptr;

		for_each_cell_compat (ptr, cur_jobs_list, cur_ptr)
			terminate_and_cleanup_job(lfirst(ptr));
	}

	if (new_ptr != NULL)
	{
		/* Then there are more new jobs. Initialize all of them. */
		ListCell *ptr;

		for_each_cell_compat (ptr, new_jobs, new_ptr)
			scheduled_bgw_job_transition_state_to(lfirst(ptr), JOB_STATE_SCHEDULED);
	}

	/* Free the old list */
	list_free_deep(cur_jobs_list);
	return new_jobs;
}

#ifdef TS_DEBUG

/* Only used by test code */
void
ts_populate_scheduled_job_tuple(ScheduledBgwJob *sjob, Datum *values)
{
	if (sjob == NULL)
		return;

	values[0] = Int32GetDatum(sjob->job.fd.id);
	values[1] = NameGetDatum(&sjob->job.fd.application_name);
	values[2] = IntervalPGetDatum(&sjob->job.fd.schedule_interval);
	values[3] = IntervalPGetDatum(&sjob->job.fd.max_runtime);
	values[4] = Int32GetDatum(sjob->job.fd.max_retries);
	values[5] = IntervalPGetDatum(&sjob->job.fd.retry_period);
	values[6] = TimestampTzGetDatum(sjob->next_start);
	values[7] = TimestampTzGetDatum(sjob->timeout_at);
	values[8] = BoolGetDatum(sjob->reserved_worker);
	values[9] = BoolGetDatum(sjob->may_need_mark_end);
}
#endif

static int
#if PG13_LT
cmp_next_start(const void *left, const void *right)
{
	const ListCell *left_cell = *((ListCell **) left);
	const ListCell *right_cell = *((ListCell **) right);
#else
cmp_next_start(const ListCell *left_cell, const ListCell *right_cell)
{
#endif
	ScheduledBgwJob *left_sjob = lfirst(left_cell);
	ScheduledBgwJob *right_sjob = lfirst(right_cell);

	if (left_sjob->next_start < right_sjob->next_start)
		return -1;

	if (left_sjob->next_start > right_sjob->next_start)
		return 1;

	return 0;
}

static void
start_scheduled_jobs(register_background_worker_callback_type bgw_register)
{
	List *ordered_scheduled_jobs;
	ListCell *lc;
	Assert(CurrentMemoryContext == scratch_mctx);

	/* Order jobs by increasing next_start */
#if PG13_LT
	ordered_scheduled_jobs = list_qsort(scheduled_jobs, cmp_next_start);
#else
	/* PG13 does in-place sort */
	ordered_scheduled_jobs = scheduled_jobs;
	list_sort(ordered_scheduled_jobs, cmp_next_start);
#endif

	foreach (lc, ordered_scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_SCHEDULED &&
			sjob->next_start <= ts_timer_get_current_timestamp())
			scheduled_ts_bgw_job_start(sjob, bgw_register);
	}
}

/* Returns the earliest time the scheduler should start a job that is waiting to be started */
static TimestampTz
earliest_wakeup_to_start_next_job()
{
	ListCell *lc;
	TimestampTz earliest = DT_NOEND;
	TimestampTz now = ts_timer_get_current_timestamp();

	foreach (lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_SCHEDULED)
		{
			TimestampTz start = sjob->next_start;
			/* if the start is less than now, this means we tried and failed to start it already, so
			 * use the retry period */
			if (start < now)
				start = TimestampTzPlusMilliseconds(now, START_RETRY_MS);
			earliest = least_timestamp(earliest, start);
		}
	}
	return earliest;
}

/* Returns the earliest time the scheduler needs to kill a job according to its timeout  */
static TimestampTz
earliest_job_timeout()
{
	ListCell *lc;
	TimestampTz earliest = DT_NOEND;

	foreach (lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_STARTED)
			earliest = least_timestamp(earliest, sjob->timeout_at);
	}
	return earliest;
}

/* Special exit function only used in shmem_exit_callback.
 * Do not call the normal cleanup function (worker_state_cleanup), because
 * 1) we do not wait for the BGW to terminate,
 * 2) we cannot access the database at this time, so we should not be
 *    trying to update the bgw_stat table.
 */
static void
terminate_all_jobs_and_release_workers()
{
	ListCell *lc;

	foreach (lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		/*
		 * Clean up the background workers. Don't worry about state of the
		 * sjobs, because this callback might have interrupted a state
		 * transition.
		 */
		if (sjob->handle != NULL)
			TerminateBackgroundWorker(sjob->handle);

		if (sjob->reserved_worker)
		{
			ts_bgw_worker_release();
			sjob->reserved_worker = false;
		}
	}
}

static void
wait_for_all_jobs_to_shutdown()
{
	ListCell *lc;

	foreach (lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_STARTED || sjob->state == JOB_STATE_TERMINATING)
			WaitForBackgroundWorkerShutdown(sjob->handle);
	}
}

static void
check_for_stopped_and_timed_out_jobs()
{
	ListCell *lc;

	foreach (lc, scheduled_jobs)
	{
		BgwHandleStatus status;
		pid_t pid;
		ScheduledBgwJob *sjob = lfirst(lc);
		TimestampTz now = ts_timer_get_current_timestamp();

		if (sjob->state != JOB_STATE_STARTED && sjob->state != JOB_STATE_TERMINATING)
			continue;

		status = GetBackgroundWorkerPid(sjob->handle, &pid);

		switch (status)
		{
			case BGWH_POSTMASTER_DIED:
				bgw_scheduler_on_postmaster_death();
				break;
			case BGWH_NOT_YET_STARTED:
				elog(ERROR, "unexpected bgworker state %d", status);
				break;
			case BGWH_STARTED:
				/* still running */
				if (sjob->state == JOB_STATE_STARTED && now >= sjob->timeout_at)
				{
					elog(WARNING,
						 "terminating background worker \"%s\" due to timeout",
						 NameStr(sjob->job.fd.application_name));
					scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_TERMINATING);
					Assert(sjob->state != JOB_STATE_STARTED);
				}
				break;
			case BGWH_STOPPED:
				StartTransactionCommand();
				scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
				CommitTransactionCommand();
				MemoryContextSwitchTo(scratch_mctx);
				Assert(sjob->state != JOB_STATE_STARTED);
				break;
		}
	}
}

/* This is the guts of the scheduler which runs the main loop.
 * The parameter ttl_ms gives a maximum time to run the loop (after which
 * the loop will exit). This functionality is used to ease testing.
 * In production, ttl_ms should be < 0 to signal that the loop should
 * run forever (or until the process gets a signal).
 *
 * The scheduler uses 2 memory contexts for its operation: scheduler_mctx
 * for long-lived objects and scratch_mctx for short-lived objects.
 * After every iteration of the scheduling main loop scratch_mctx gets
 * reset. Special care needs to be taken in regards to memory contexts
 * since StartTransactionCommand creates and switches to a transaction
 * memory context which gets deleted on CommitTransactionCommand which
 * switches CurrentMemoryContext back to TopMemoryContext. So operations
 * wrapped in Start/CommitTransactionCommit will not happen in scratch_mctx
 * but will get freed on CommitTransactionCommand.
 */
void
ts_bgw_scheduler_process(int32 run_for_interval_ms,
						 register_background_worker_callback_type bgw_register)
{
	TimestampTz start = ts_timer_get_current_timestamp();
	TimestampTz quit_time = DT_NOEND;

	pgstat_report_activity(STATE_RUNNING, NULL);

	/* txn to read the list of jobs from the DB */
	StartTransactionCommand();
	scheduled_jobs = ts_update_scheduled_jobs_list(scheduled_jobs, scheduler_mctx);
	CommitTransactionCommand();
	MemoryContextSwitchTo(scratch_mctx);

	jobs_list_needs_update = false;

	if (run_for_interval_ms > 0)
		quit_time = TimestampTzPlusMilliseconds(start, run_for_interval_ms);

	ereport(DEBUG1, (errmsg("database scheduler starting for database %u", MyDatabaseId)));

	/*
	 * on SIGTERM the process will usually die from the CHECK_FOR_INTERRUPTS
	 * in the die() called from the sigterm handler. Child reaping is then
	 * handled in the before_shmem_exit,
	 * bgw_scheduler_before_shmem_exit_callback.
	 */
	while (quit_time > ts_timer_get_current_timestamp() && !ProcDiePending && !ts_shutdown_bgw)
	{
		TimestampTz next_wakeup = quit_time;
		Assert(CurrentMemoryContext == scratch_mctx);

		/* start jobs, and then check when to next wake up */
		start_scheduled_jobs(bgw_register);
		next_wakeup = least_timestamp(next_wakeup, earliest_wakeup_to_start_next_job());
		next_wakeup = least_timestamp(next_wakeup, earliest_job_timeout());

		pgstat_report_activity(STATE_IDLE, NULL);
		ts_timer_wait(next_wakeup);
		pgstat_report_activity(STATE_RUNNING, NULL);

		CHECK_FOR_INTERRUPTS();

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Process any cache invalidation message that indicates we need to
		 * update the jobs list
		 */
		AcceptInvalidationMessages();

		if (jobs_list_needs_update)
		{
			StartTransactionCommand();
			Assert(CurrentMemoryContext == CurTransactionContext);
			scheduled_jobs = ts_update_scheduled_jobs_list(scheduled_jobs, scheduler_mctx);
			CommitTransactionCommand();
			MemoryContextSwitchTo(scratch_mctx);
			jobs_list_needs_update = false;
		}

		check_for_stopped_and_timed_out_jobs();

		MemoryContextReset(scratch_mctx);
	}

#ifdef TS_DEBUG
	if (ts_shutdown_bgw)
		elog(WARNING, "bgw scheduler stopped due to shutdown_bgw guc");
#endif

	CHECK_FOR_INTERRUPTS();

	wait_for_all_jobs_to_shutdown();
	check_for_stopped_and_timed_out_jobs();
}

static void
bgw_scheduler_before_shmem_exit_callback(int code, Datum arg)
{
	terminate_all_jobs_and_release_workers();
}

void
ts_bgw_scheduler_setup_callbacks()
{
	before_shmem_exit(bgw_scheduler_before_shmem_exit_callback, PointerGetDatum(NULL));
}

/* some of the scheduler mock code calls functions from this file without going through
 * the main loop so we need a way to setup the memory contexts
 */
void
ts_bgw_scheduler_setup_mctx()
{
	scheduler_mctx = AllocSetContextCreate(TopMemoryContext, "Scheduler", ALLOCSET_DEFAULT_SIZES);
	scratch_mctx =
		AllocSetContextCreate(scheduler_mctx, "SchedulerScratch", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(scratch_mctx);
}

static void handle_sigterm(SIGNAL_ARGS)
{
	/*
	 * do not use a level >= ERROR because we don't want to exit here but
	 * rather only during CHECK_FOR_INTERRUPTS
	 */
	ereport(LOG,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating TimescaleDB job scheduler due to administrator command")));
	die(postgres_signal_arg);
}

static void handle_sighup(SIGNAL_ARGS)
{
	/* based on av_sighup_handler */
	int save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Register SIGTERM and SIGHUP handlers for bgw_scheduler.
 * This function _must_ be called with signals blocked, i.e., after calling
 * BackgroundWorkerBlockSignals
 */
void
ts_bgw_scheduler_register_signal_handlers(void)
{
	/*
	 * do not use the default `bgworker_die` sigterm handler because it does
	 * not respect critical sections
	 */
	pqsignal(SIGTERM, handle_sigterm);
	pqsignal(SIGHUP, handle_sighup);

	/* Some SIGHUPS may already have been dropped, so we must load the file here */
	got_SIGHUP = false;
	ProcessConfigFile(PGC_SIGHUP);
}

Datum
ts_bgw_scheduler_main(PG_FUNCTION_ARGS)
{
	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */
	ts_bgw_scheduler_register_signal_handlers();
	BackgroundWorkerUnblockSignals();

	ts_bgw_scheduler_setup_callbacks();

	pgstat_report_appname(SCHEDULER_APPNAME);

	ts_bgw_scheduler_setup_mctx();

	ts_bgw_scheduler_process(-1, NULL);

	Assert(scheduled_jobs == NIL);
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(scheduler_mctx);

	PG_RETURN_VOID();
};

void
ts_bgw_job_cache_invalidate_callback()
{
	jobs_list_needs_update = true;
}

/*
 * This is a scheduler that takes background jobs and schedules them appropriately
 *
 * Limitations: For now the jobs are only loaded when the scheduler starts and are not
 * updated if the jobs table changes
 *
 * TODO: right now jobs are not prioritized when slots available. Should prioritize in
 * asc next_start order.
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
#include <utils/jsonb.h>
#include <utils/timestamp.h>
#include <utils/snapmgr.h>
#include <utils/memutils.h>
#include <access/xact.h>
#include <pgstat.h>
#include <tcop/tcopprot.h>

#include "extension.h"
#include "scheduler.h"
#include "job.h"
#include "job_stat.h"
#include "version.h"
#include "compat.h"
#include "timer.h"
#include "launcher_interface.h"

#define SCHEDULER_APPNAME "TimescaleDB Background Worker Scheduler"

static TimestampTz
least_timestamp(TimestampTz left, TimestampTz right)
{
	return (left < right ? left : right);
}

TS_FUNCTION_INFO_V1(ts_bgw_scheduler_main);


/* has to be global to shutdown jobs on exit */
static List *scheduled_jobs = NIL;

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
	BgwJob		job;
	TimestampTz next_start;
	TimestampTz timeout_at;
	JobState	state;
	BackgroundWorkerHandle *handle;

	bool		reserved_worker;

	/*
	 * We say "may" here since under normal circumstances the job itself will
	 * perform the mark_end
	 */
	bool		may_need_mark_end;
} ScheduledBgwJob;


static void on_failure_to_start_job(ScheduledBgwJob *sjob);

static void
mark_job_as_started(ScheduledBgwJob *sjob)
{
	Assert(!sjob->may_need_mark_end);
	bgw_job_stat_mark_start(sjob->job.fd.id);
	sjob->may_need_mark_end = true;
}

static void
mark_job_as_ended(ScheduledBgwJob *sjob, JobResult res)
{
	Assert(sjob->may_need_mark_end);
	bgw_job_stat_mark_end(&sjob->job, res);
	sjob->may_need_mark_end = false;
}

static void
worker_state_cleanup(ScheduledBgwJob *sjob)
{
	/*
	 * This function needs to be safe wrt failures occuring at any point in
	 * the job starting process.
	 */

	/*
	 * first cleanup reserved workers before accessing db. Want to minimize
	 * the possibility of errors before worker is released
	 */
	if (sjob->reserved_worker)
	{
		bgw_worker_release();
		sjob->reserved_worker = false;
	}

	if (sjob->may_need_mark_end)
	{
		BgwJobStat *job_stat = bgw_job_stat_find(sjob->job.fd.id);

		Assert(job_stat != NULL);

		if (!bgw_job_stat_end_was_marked(job_stat))
		{
			/*
			 * Usually the job process will mark the end, but if the job gets
			 * a signal (cancel or terminate), it won't be able to so we
			 * should.
			 */
			mark_job_as_ended(sjob, JOB_FAILURE);
			/* reload updated value */
			job_stat = bgw_job_stat_find(sjob->job.fd.id);
		}
		else
		{
			sjob->may_need_mark_end = false;
		}
	}
}


#if USE_ASSERT_CHECKING
static void
assert_that_worker_has_stopped(ScheduledBgwJob *sjob)
{
	pid_t		pid;
	BgwHandleStatus status;

	Assert(sjob->reserved_worker);
	status = GetBackgroundWorkerPid(sjob->handle, &pid);
	Assert(BGWH_STOPPED == status);
}
#endif

/* Set the state of the job.
* This function is responsible for setting all of the variables in ScheduledBgwJob
* except for the job itself.
*/
static void
scheduled_bgw_job_transition_state_to(ScheduledBgwJob *sjob, JobState new_state)
{
#if USE_ASSERT_CHECKING
	JobState	prev_state = sjob->state;
#endif

	BgwJobStat *job_stat;

	switch (new_state)
	{
		case JOB_STATE_DISABLED:
			Assert(prev_state == JOB_STATE_STARTED || prev_state == JOB_STATE_TERMINATING);
			sjob->handle = NULL;
			break;
		case JOB_STATE_SCHEDULED:
			/* prev_state can be any value, including itself */
#if USE_ASSERT_CHECKING
			/* Sanity check: worker has stopped (if it was started) */
			if (sjob->handle != NULL)
				assert_that_worker_has_stopped(sjob);
#endif
			worker_state_cleanup(sjob);

			job_stat = bgw_job_stat_find(sjob->job.fd.id);

			if (!bgw_job_stat_should_execute(job_stat, &sjob->job))
			{
				scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_DISABLED);
				return;
			}

			Assert(!sjob->reserved_worker);
			sjob->next_start = bgw_job_stat_next_start(job_stat, &sjob->job);
			sjob->handle = NULL;
			break;
		case JOB_STATE_STARTED:
			Assert(prev_state == JOB_STATE_SCHEDULED);
			Assert(sjob->handle == NULL);
			Assert(!sjob->reserved_worker);

			StartTransactionCommand();

			/*
			 * start the job before you can encounter any errors so that they
			 * are always registerd
			 */
			mark_job_as_started(sjob);
			if (bgw_job_has_timeout(&sjob->job))
				sjob->timeout_at = bgw_job_timeout_at(&sjob->job, timer_get_current_timestamp());
			else
				sjob->timeout_at = DT_NOEND;
			CommitTransactionCommand();

			sjob->reserved_worker = bgw_worker_reserve();
			if (!sjob->reserved_worker)
			{
				elog(WARNING, "failed to launch job %d \"%s\": out of background workers", sjob->job.fd.id, NameStr(sjob->job.fd.application_name));
				on_failure_to_start_job(sjob);
				return;
			}

			elog(DEBUG1, "launching job %d \"%s\"", sjob->job.fd.id, NameStr(sjob->job.fd.application_name));

			sjob->handle = bgw_job_start(&sjob->job);
			if (sjob->handle == NULL)
			{
				elog(WARNING, "failed to launch job %d \"%s\": failed to start a background worker", sjob->job.fd.id, NameStr(sjob->job.fd.application_name));
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
	mark_job_as_ended(sjob, JOB_FAILURE);
	scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
	CommitTransactionCommand();
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
scheduled_bgw_job_start(ScheduledBgwJob *sjob, register_background_worker_callback_type bgw_register)
{
	pid_t		pid;
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
			break;
		case BGWH_NOT_YET_STARTED:
			/* should not be possible */
			elog(ERROR, "unexpected bgworker state %d", status);
			break;
	}
}

static List *
initialize_scheduled_jobs_list(MemoryContext mctx)
{
	List	   *jobs = bgw_job_get_all(sizeof(ScheduledBgwJob), mctx);
	ListCell   *lc;

	foreach(lc, jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
	}
	return jobs;
}

static void
start_scheduled_jobs(register_background_worker_callback_type bgw_register)
{
	ListCell   *lc;

	foreach(lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_SCHEDULED && sjob->next_start <= timer_get_current_timestamp())
			scheduled_bgw_job_start(sjob, bgw_register);
	}
}

/* Returns the earliest time the scheduler should start a job that is waiting to be started */
static TimestampTz
earliest_time_to_start_next_job()
{
	ListCell   *lc;
	TimestampTz earliest = DT_NOEND;

	foreach(lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_SCHEDULED)
			earliest = least_timestamp(earliest, sjob->next_start);
	}
	return earliest;
}

/* Returns the earliest time the scheduler needs to kill a job according to its timeout  */
static TimestampTz
earliest_job_timeout()
{
	ListCell   *lc;
	TimestampTz earliest = DT_NOEND;

	foreach(lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_STARTED)
			earliest = least_timestamp(earliest, sjob->timeout_at);
	}
	return earliest;
}

static void
terminate_all_jobs_and_release_workers()
{
	ListCell   *lc;

	foreach(lc, scheduled_jobs)
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
			bgw_worker_release();
			sjob->reserved_worker = false;
		}
	}
}

static void
wait_for_all_jobs_to_shutdown()
{
	ListCell   *lc;

	foreach(lc, scheduled_jobs)
	{
		ScheduledBgwJob *sjob = lfirst(lc);

		if (sjob->state == JOB_STATE_STARTED || sjob->state == JOB_STATE_TERMINATING)
			WaitForBackgroundWorkerShutdown(sjob->handle);
	}
}

static void
check_for_stopped_and_timed_out_jobs()
{
	ListCell   *lc;

	foreach(lc, scheduled_jobs)
	{
		BgwHandleStatus status;
		pid_t		pid;
		ScheduledBgwJob *sjob = lfirst(lc);
		TimestampTz now = timer_get_current_timestamp();

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
					elog(WARNING, "terminating background worker \"%s\" due to timeout", NameStr(sjob->job.fd.application_name));
					scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_TERMINATING);
					Assert(sjob->state != JOB_STATE_STARTED);
				}
				break;
			case BGWH_STOPPED:
				StartTransactionCommand();
				scheduled_bgw_job_transition_state_to(sjob, JOB_STATE_SCHEDULED);
				CommitTransactionCommand();
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
 */
void
bgw_scheduler_process(int32 run_for_interval_ms, register_background_worker_callback_type bgw_register)
{
	TimestampTz start = timer_get_current_timestamp();
	TimestampTz quit_time = DT_NOEND;
	MemoryContext scheduler_mctx = CurrentMemoryContext;

	/* txn to read the list of jobs from the DB */
	StartTransactionCommand();
	scheduled_jobs = initialize_scheduled_jobs_list(scheduler_mctx);
	CommitTransactionCommand();

	if (run_for_interval_ms > 0)
		quit_time = TimestampTzPlusMilliseconds(start, run_for_interval_ms);

	ereport(DEBUG1, (errmsg("database scheduler starting for database %d", MyDatabaseId)));
	while (quit_time > timer_get_current_timestamp())
	{
		TimestampTz next_wakeup = quit_time;

		/* start jobs, and then check when to next wake up */
		start_scheduled_jobs(bgw_register);
		next_wakeup = least_timestamp(next_wakeup, earliest_time_to_start_next_job());
		next_wakeup = least_timestamp(next_wakeup, earliest_job_timeout());

		timer_wait(next_wakeup);

		CHECK_FOR_INTERRUPTS();

		check_for_stopped_and_timed_out_jobs();
	}

	wait_for_all_jobs_to_shutdown();
	check_for_stopped_and_timed_out_jobs();
}

static void
bgw_scheduler_before_shmem_exit_callback(int code, Datum arg)
{
	terminate_all_jobs_and_release_workers();
}

void
bgw_scheduler_setup_callbacks()
{
	before_shmem_exit(bgw_scheduler_before_shmem_exit_callback, PointerGetDatum(NULL));
}

static void
handle_sigterm(SIGNAL_ARGS)
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

Datum
ts_bgw_scheduler_main(PG_FUNCTION_ARGS)
{
	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */

	/*
	 * do not use the default `bgworker_die` sigterm handler because it does
	 * not respect critical sections
	 */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	bgw_scheduler_setup_callbacks();

	pgstat_report_appname(SCHEDULER_APPNAME);

	bgw_scheduler_process(-1, NULL);

	PG_RETURN_VOID();
};

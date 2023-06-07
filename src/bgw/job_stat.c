/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <utils/fmgrprotos.h>

#include <stdlib.h>
#include <math.h>

#include "job_stat.h"
#include "scanner.h"
#include "timer.h"
#include "utils.h"
#include "jsonb_utils.h"
#include <utils/builtins.h>
#include <utils/resowner.h>
#include "time_bucket.h"

#define MAX_INTERVALS_BACKOFF 5
#define MAX_FAILURES_MULTIPLIER 20
#define MIN_WAIT_AFTER_CRASH_MS (5 * 60 * 1000)

static bool
bgw_job_stat_next_start_was_set(FormData_bgw_job_stat *fd)
{
	return fd->next_start != DT_NOBEGIN;
}

static ScanTupleResult
bgw_job_stat_tuple_found(TupleInfo *ti, void *const data)
{
	BgwJobStat **job_stat_pp = data;

	*job_stat_pp = STRUCT_FROM_SLOT(ti->slot, ti->mctx, BgwJobStat, FormData_bgw_job_stat);

	/*
	 * Return SCAN_CONTINUE because we check for multiple tuples as an error
	 * condition.
	 */
	return SCAN_CONTINUE;
}

static bool
bgw_job_stat_scan_one(int indexid, ScanKeyData scankey[], int nkeys, tuple_found_func tuple_found,
					  tuple_filter_func tuple_filter, void *data, LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB_STAT),
		.index = catalog_get_index(catalog, BGW_JOB_STAT, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.flags = SCANNER_F_KEEPLOCK,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan_one(&scanctx, false, "bgw job stat");
}

static inline bool
bgw_job_stat_scan_job_id(int32 bgw_job_id, tuple_found_func tuple_found,
						 tuple_filter_func tuple_filter, void *data, LOCKMODE lockmode)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_stat_pkey_idx_job_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(bgw_job_id));
	return bgw_job_stat_scan_one(BGW_JOB_STAT_PKEY_IDX,
								 scankey,
								 1,
								 tuple_found,
								 tuple_filter,
								 data,
								 lockmode);
}

TSDLLEXPORT BgwJobStat *
ts_bgw_job_stat_find(int32 bgw_job_id)
{
	BgwJobStat *job_stat = NULL;

	bgw_job_stat_scan_job_id(bgw_job_id,
							 bgw_job_stat_tuple_found,
							 NULL,
							 &job_stat,
							 AccessShareLock);

	return job_stat;
}

static ScanTupleResult
bgw_job_stat_tuple_delete(TupleInfo *ti, void *const data)
{
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));

	return SCAN_CONTINUE;
}

void
ts_bgw_job_stat_delete(int32 bgw_job_id)
{
	bgw_job_stat_scan_job_id(bgw_job_id,
							 bgw_job_stat_tuple_delete,
							 NULL,
							 NULL,
							 ShareRowExclusiveLock);
}

/* Mark the start of a job. This should be done in a separate transaction by the scheduler
 *  before the bgw for a job is launched. This ensures that the job is counted as started
 * before /any/ job specific code is executed. A job that has been started but never ended
 * is assumed to have crashed. We use this conservative design since no process in the database
 * instance can write once a crash happened in any job. Therefore our only choice is to deduce
 * a crash from the lack of a write (the marked end write in this case).
 */
static ScanTupleResult
bgw_job_stat_tuple_mark_start(TupleInfo *ti, void *const data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple = heap_copytuple(tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	fd->last_start = ts_timer_get_current_timestamp();
	fd->last_finish = DT_NOBEGIN;
	fd->next_start = DT_NOBEGIN;

	fd->total_runs++;

	/*
	 * This is undone by any of the end marks. This is so that we count
	 * crashes conservatively. Pretty much the crash is incremented in the
	 * beginning and then decremented during `bgw_job_stat_tuple_mark_end`.
	 * Thus, it only remains incremented if the job is never marked as having
	 * ended. This happens when: 1) the job crashes 2) another process crashes
	 * while the job is running 3) the scheduler gets a SIGTERM while the job
	 * is running
	 *
	 * Unfortunately, 3 cannot be helped because when a scheduler gets a
	 * SIGTERM it sends SIGTERMS to it's any running jobs as well. Since you
	 * aren't supposed to write to the DB Once you get a sigterm, neither the
	 * job nor the scheduler can mark the end of a job.
	 */
	fd->last_run_success = false;
	fd->total_crashes++;
	fd->consecutive_crashes++;
	fd->flags = ts_clear_flags_32(fd->flags, LAST_CRASH_REPORTED);
	ts_catalog_update(ti->scanrel, new_tuple);
	heap_freetuple(new_tuple);

	return SCAN_DONE;
}

typedef struct
{
	JobResult result;
	BgwJob *job;
} JobResultCtx;

/*
 * logic is the following
 * Ideally we would return
 * time_bucket(schedule_interval, finish_time, origin => initial_start, timezone).
 * That is what we return when the schedule interval does not have month components.
 * However, when there is a month component in the schedule interval,
 * then supplying the origin in time_bucket
 * does not work and the returned bucket is aligned on the start of the month.
 * In those cases, we only have month components. So we compute the difference in
 * months between the initial_start's timebucket and the finish time's bucket.
 */
TimestampTz
ts_get_next_scheduled_execution_slot(BgwJob *job, TimestampTz finish_time)
{
	Assert(job->fd.fixed_schedule == true);
	Datum schedint_datum = IntervalPGetDatum(&job->fd.schedule_interval);
	Datum timebucket_fini, timebucket_init, result;

	Interval one_month = {
		.day = 0,
		.time = 0,
		.month = 1,
	};

	if (job->fd.schedule_interval.month > 0)
	{
		if (job->fd.timezone == NULL)
		{
			timebucket_init = DirectFunctionCall2(ts_timestamptz_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(job->fd.initial_start));
			timebucket_fini = DirectFunctionCall2(ts_timestamptz_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time));
		}
		else
		{
			char *tz = text_to_cstring(job->fd.timezone);
			timebucket_fini = DirectFunctionCall3(ts_timestamptz_timezone_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time),
												  CStringGetTextDatum(tz));

			timebucket_init = DirectFunctionCall3(ts_timestamptz_timezone_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(job->fd.initial_start),
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
									 TimestampTzGetDatum(job->fd.initial_start),
									 months_to_add);
	}
	else
	{
		if (job->fd.timezone == NULL)
		{
			/* it is safe to use the origin in time_bucket calculation */
			timebucket_fini = DirectFunctionCall3(ts_timestamptz_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time),
												  TimestampTzGetDatum(job->fd.initial_start));
			result = timebucket_fini;
		}
		else
		{
			char *tz = text_to_cstring(job->fd.timezone);
			timebucket_fini = DirectFunctionCall4(ts_timestamptz_timezone_bucket,
												  schedint_datum,
												  TimestampTzGetDatum(finish_time),
												  CStringGetTextDatum(tz),
												  TimestampTzGetDatum(job->fd.initial_start));
			result = timebucket_fini;
		}
	}
	while (DatumGetTimestampTz(result) <= finish_time)
	{
		result = DirectFunctionCall2(timestamptz_pl_interval, result, schedint_datum);
	}
	return DatumGetTimestampTz(result);
}

static TimestampTz
calculate_next_start_on_success_fixed(TimestampTz finish_time, BgwJob *job)
{
	TimestampTz next_slot;

	next_slot = ts_get_next_scheduled_execution_slot(job, finish_time);

	return next_slot;
}

static TimestampTz
calculate_next_start_on_success_drifting(TimestampTz last_finish, BgwJob *job)
{
	TimestampTz ts;
	ts = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
												 TimestampTzGetDatum(last_finish),
												 IntervalPGetDatum(&job->fd.schedule_interval)));
	return ts;
}

static TimestampTz
calculate_next_start_on_success(TimestampTz finish_time, BgwJob *job)
{
	/* next_start is the previously calculated next_start for this job */
	TimestampTz ts;
	TimestampTz last_finish = finish_time;
	if (!IS_VALID_TIMESTAMP(finish_time))
	{
		last_finish = ts_timer_get_current_timestamp();
	}

	/* calculate next_start differently depending on drift/no drift */
	if (job->fd.fixed_schedule)
		ts = calculate_next_start_on_success_fixed(last_finish, job);
	else
		ts = calculate_next_start_on_success_drifting(last_finish, job);

	return ts;
}

static float8
calculate_jitter_percent()
{
	/* returns a number in the range [-0.125, 0.125] */
	uint8 percent = rand();
	return ldexp((double) (16 - (int) (percent % 32)), -7);
}

/* For failures we have backoff based on consecutive failures
 * along with a ceiling at schedule_interval * MAX_INTERVALS_BACKOFF / 1 minute
 * for jobs failing at runtime / for jobs failing to launch.
 * We also limit the backoff in case of consecutive failures as we don't
 * want to pass in input that leads to out of range timestamps and don't want to
 * put off the next start time for the job indefinitely
 */
static TimestampTz
calculate_next_start_on_failure(TimestampTz finish_time, int consecutive_failures, BgwJob *job,
								bool launch_failure)
{
	float8 jitter = calculate_jitter_percent();

	/*
	 * Have to be declared volatile because they are modified between
	 * setjmp/longjmp calls.
	 */
	volatile TimestampTz res = 0;
	volatile bool res_set = false;
	volatile TimestampTz last_finish = finish_time;

	/* consecutive failures includes this failure */
	float8 multiplier = (consecutive_failures > MAX_FAILURES_MULTIPLIER ? MAX_FAILURES_MULTIPLIER :
																		  consecutive_failures);
	Assert(consecutive_failures > 0 && multiplier < 63);

	MemoryContext oldctx = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	/* 2^(consecutive_failures) - 1, at most 2^20 */
	int64 max_slots = (INT64CONST(1) << (int64) multiplier) - INT64CONST(1);
	int64 rand_backoff = rand() % (max_slots * USECS_PER_SEC);

	if (!IS_VALID_TIMESTAMP(finish_time))
	{
		elog(LOG, "%s: invalid finish time", __func__);
		last_finish = ts_timer_get_current_timestamp();
	}

	PG_TRY();
	{
		Datum ival;
		/* ival_max is the ceiling = MAX_INTERVALS_BACKOFF * schedule_interval */
		Datum ival_max;
		// max wait time to launch job is 1 minute
		Interval interval_max = { .time = 60000000 };
		Interval retry_ival = { .time = 2000000 };
		retry_ival.time += rand_backoff;

		BeginInternalSubTransaction("next start on failure");

		if (launch_failure)
		{
			// random backoff seconds in [2, 2 + 2^f]
			ival = IntervalPGetDatum(&retry_ival);
			ival_max = IntervalPGetDatum(&interval_max);
		}
		else
		{
			/* ival = retry_period * (consecutive_failures)  */
			ival = DirectFunctionCall2(interval_mul,
									   IntervalPGetDatum(&job->fd.retry_period),
									   Float8GetDatum(multiplier));
			/* ival_max is the ceiling = MAX_INTERVALS_BACKOFF * schedule_interval */
			ival_max = DirectFunctionCall2(interval_mul,
										   IntervalPGetDatum(&job->fd.schedule_interval),
										   Float8GetDatum(MAX_INTERVALS_BACKOFF));
		}

		if (DatumGetInt32(DirectFunctionCall2(interval_cmp, ival, ival_max)) > 0)
			ival = ival_max;

		/* Add some random jitter to prevent stampeding-herds, interval will be within about +-13%
		 */
		ival = DirectFunctionCall2(interval_mul, ival, Float8GetDatum(1.0 + jitter));

		res = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval, TimestampTzGetDatum(last_finish), ival));
		res_set = true;
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldctx);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldctx);
		CurrentResourceOwner = oldowner;
		ErrorData *errdata = CopyErrorData();
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not calculate next start on failure: resetting value"),
				 errdetail("Error: %s.", errdata->message)));
		FlushErrorState();
	}
	PG_END_TRY();
	Assert(CurrentMemoryContext == oldctx);
	if (!res_set)
	{
		TimestampTz nowt;
		/* job->fd_retry_period is a valid non-null value */
		nowt = ts_timer_get_current_timestamp();
		res = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
													  TimestampTzGetDatum(nowt),
													  IntervalPGetDatum(&job->fd.retry_period)));
	}
	/* for fixed_schedules, we make sure that if the calculated next_start time
	 * surpasses the next scheduled slot, then next_start will be set to the value
	 * of the next scheduled slot, so we don't get off track */
	if (job->fd.fixed_schedule)
	{
		TimestampTz next_slot = ts_get_next_scheduled_execution_slot(job, finish_time);
		if (res > next_slot)
			res = next_slot;
	}
	return res;
}

static TimestampTz
calculate_next_start_on_failed_launch(int consecutive_failed_launches, BgwJob *job)
{
	TimestampTz now = ts_timer_get_current_timestamp();
	TimestampTz failure_calc =
		calculate_next_start_on_failure(now, consecutive_failed_launches, job, true);

	return failure_calc;
}

/* For crashes, the logic is the similar as for failures except we also have
 *  a minimum wait after a crash that we wait, so that if an operator needs to disable the job,
 *  there will be enough time before another crash.
 */
static TimestampTz
calculate_next_start_on_crash(int consecutive_crashes, BgwJob *job)
{
	TimestampTz now = ts_timer_get_current_timestamp();
	TimestampTz failure_calc =
		calculate_next_start_on_failure(now, consecutive_crashes, job, false);
	TimestampTz min_time = TimestampTzPlusMilliseconds(now, MIN_WAIT_AFTER_CRASH_MS);

	if (min_time > failure_calc)
		return min_time;
	return failure_calc;
}

static ScanTupleResult
bgw_job_stat_tuple_mark_end(TupleInfo *ti, void *const data)
{
	JobResultCtx *result_ctx = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple = heap_copytuple(tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(new_tuple);
	Interval *duration;

	if (should_free)
		heap_freetuple(tuple);

	fd->last_finish = ts_timer_get_current_timestamp();

	duration = DatumGetIntervalP(DirectFunctionCall2(timestamp_mi,
													 TimestampTzGetDatum(fd->last_finish),
													 TimestampTzGetDatum(fd->last_start)));

	/* undo marking created by start marks */
	fd->last_run_success = result_ctx->result == JOB_SUCCESS ? true : false;
	fd->total_crashes--;
	fd->consecutive_crashes = 0;
	fd->flags = ts_clear_flags_32(fd->flags, LAST_CRASH_REPORTED);

	if (result_ctx->result == JOB_SUCCESS)
	{
		fd->total_success++;
		fd->consecutive_failures = 0;
		fd->last_successful_finish = fd->last_finish;
		fd->total_duration =
			*DatumGetIntervalP(DirectFunctionCall2(interval_pl,
												   IntervalPGetDatum(&fd->total_duration),
												   IntervalPGetDatum(duration)));
		/* Mark the next start at the end if the job itself hasn't */
		if (!bgw_job_stat_next_start_was_set(fd))
			fd->next_start = calculate_next_start_on_success(fd->last_finish, result_ctx->job);
	}
	else
	{
		fd->total_failures++;
		fd->consecutive_failures++;
		fd->total_duration_failures =
			*DatumGetIntervalP(DirectFunctionCall2(interval_pl,
												   IntervalPGetDatum(&fd->total_duration_failures),
												   IntervalPGetDatum(duration)));

		/*
		 * Mark the next start at the end if the job itself hasn't (this may
		 * have happened before failure) and the failure was not in starting.
		 * If the failure was in starting, then next_start should have been
		 * restored in `on_failure_to_start_job` and thus we don't change it here.
		 * Even if it wasn't restored, then keep it as DT_NOBEGIN to mark it as highest priority.
		 */
		if (!bgw_job_stat_next_start_was_set(fd) && result_ctx->result != JOB_FAILURE_TO_START)
			fd->next_start = calculate_next_start_on_failure(fd->last_finish,
															 fd->consecutive_failures,
															 result_ctx->job,
															 false);
	}

	ts_catalog_update(ti->scanrel, new_tuple);
	heap_freetuple(new_tuple);

	return SCAN_DONE;
}

static ScanTupleResult
bgw_job_stat_tuple_mark_crash_reported(TupleInfo *ti, void *const data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple = heap_copytuple(tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	fd->flags = ts_set_flags_32(fd->flags, LAST_CRASH_REPORTED);

	ts_catalog_update(ti->scanrel, new_tuple);
	heap_freetuple(new_tuple);

	return SCAN_DONE;
}

static ScanTupleResult
bgw_job_stat_tuple_set_next_start(TupleInfo *ti, void *const data)
{
	TimestampTz *next_start = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple = heap_copytuple(tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	fd->next_start = *next_start;
	ts_catalog_update(ti->scanrel, new_tuple);
	heap_freetuple(new_tuple);

	return SCAN_DONE;
}

static bool
bgw_job_stat_insert_relation(Relation rel, int32 bgw_job_id, bool mark_start,
							 TimestampTz next_start)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_bgw_job_stat];
	bool nulls[Natts_bgw_job_stat] = { false };
	CatalogSecurityContext sec_ctx;
	Interval zero_ival = {
		.time = 0,
	};

	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_job_id)] = Int32GetDatum(bgw_job_id);
	if (mark_start)
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_last_start)] =
			TimestampGetDatum(ts_timer_get_current_timestamp());
	else
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_last_start)] =
			TimestampGetDatum(DT_NOBEGIN);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_last_finish)] = TimestampGetDatum(DT_NOBEGIN);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_next_start)] = TimestampGetDatum(next_start);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_last_successful_finish)] =
		TimestampGetDatum(DT_NOBEGIN);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_runs)] =
		Int64GetDatum((mark_start ? 1 : 0));
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_duration)] =
		IntervalPGetDatum(&zero_ival);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_duration_failures)] =
		IntervalPGetDatum(&zero_ival);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_success)] = Int64GetDatum(0);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_failures)] = Int64GetDatum(0);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_consecutive_failures)] = Int32GetDatum(0);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_flags)] =
		Int32GetDatum(JOB_STAT_FLAGS_DEFAULT);

	if (mark_start)
	{
		/* This is udone by any of the end marks */
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_last_run_success)] = BoolGetDatum(false);
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_crashes)] = Int64GetDatum(1);
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_consecutive_crashes)] = Int32GetDatum(1);
	}
	else
	{
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_last_run_success)] = BoolGetDatum(true);
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_crashes)] = Int64GetDatum(0);
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_consecutive_crashes)] = Int32GetDatum(0);
	}

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	return true;
}

void
ts_bgw_job_stat_mark_start(int32 bgw_job_id)
{
	/* We grab a ShareRowExclusiveLock here because we need to ensure that no
	 * job races and adds a job when we insert the relation as well since that
	 * can trigger a failure when inserting a row for the job. We use the
	 * RowExclusiveLock in the scan since we cannot use NoLock (relation_open
	 * requires a lock that it not NoLock). */
	Relation rel =
		table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT), ShareRowExclusiveLock);
	if (!bgw_job_stat_scan_job_id(bgw_job_id,
								  bgw_job_stat_tuple_mark_start,
								  NULL,
								  NULL,
								  RowExclusiveLock))
		bgw_job_stat_insert_relation(rel, bgw_job_id, true, DT_NOBEGIN);
	table_close(rel, NoLock);
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
ts_bgw_job_stat_mark_end(BgwJob *job, JobResult result)
{
	JobResultCtx res = {
		.job = job,
		.result = result,
	};

	if (!bgw_job_stat_scan_job_id(job->fd.id,
								  bgw_job_stat_tuple_mark_end,
								  NULL,
								  &res,
								  ShareRowExclusiveLock))
		elog(ERROR, "unable to find job statistics for job %d", job->fd.id);
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
ts_bgw_job_stat_mark_crash_reported(int32 bgw_job_id)
{
	if (!bgw_job_stat_scan_job_id(bgw_job_id,
								  bgw_job_stat_tuple_mark_crash_reported,
								  NULL,
								  NULL,
								  RowExclusiveLock))
		elog(ERROR, "unable to find job statistics for job %d", bgw_job_id);
	pgstat_report_activity(STATE_IDLE, NULL);
}

bool
ts_bgw_job_stat_end_was_marked(BgwJobStat *jobstat)
{
	return !TIMESTAMP_IS_NOBEGIN(jobstat->fd.last_finish);
}

TSDLLEXPORT void
ts_bgw_job_stat_set_next_start(int32 job_id, TimestampTz next_start)
{
	/* Cannot use DT_NOBEGIN as that's the value used to indicate "not set" */
	if (next_start == DT_NOBEGIN)
		elog(ERROR, "cannot set next start to -infinity");

	if (!bgw_job_stat_scan_job_id(job_id,
								  bgw_job_stat_tuple_set_next_start,
								  NULL,
								  &next_start,
								  ShareRowExclusiveLock))
		elog(ERROR, "unable to find job statistics for job %d", job_id);
}

/* update next_start if job stat exists */
TSDLLEXPORT bool
ts_bgw_job_stat_update_next_start(int32 job_id, TimestampTz next_start, bool allow_unset)
{
	bool found = false;
	/* Cannot use DT_NOBEGIN as that's the value used to indicate "not set" */
	if (!allow_unset && next_start == DT_NOBEGIN)
		elog(ERROR, "cannot set next start to -infinity");

	found = bgw_job_stat_scan_job_id(job_id,
									 bgw_job_stat_tuple_set_next_start,
									 NULL,
									 &next_start,
									 ShareRowExclusiveLock);
	return found;
}

TSDLLEXPORT void
ts_bgw_job_stat_upsert_next_start(int32 bgw_job_id, TimestampTz next_start)
{
	/* Cannot use DT_NOBEGIN as that's the value used to indicate "not set" */
	if (next_start == DT_NOBEGIN)
		elog(ERROR, "cannot set next start to -infinity");

	/* We grab a ShareRowExclusiveLock here because we need to ensure that no
	 * job races and adds a job when we insert the relation as well since that
	 * can trigger a failure when inserting a row for the job. We use the
	 * RowExclusiveLock in the scan since we cannot use NoLock (relation_open
	 * requires a lock that it not NoLock). */
	Relation rel =
		table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT), ShareRowExclusiveLock);
	if (!bgw_job_stat_scan_job_id(bgw_job_id,
								  bgw_job_stat_tuple_set_next_start,
								  NULL,
								  &next_start,
								  RowExclusiveLock))
		bgw_job_stat_insert_relation(rel, bgw_job_id, false, next_start);
	table_close(rel, NoLock);
}

bool
ts_bgw_job_stat_should_execute(BgwJobStat *jobstat, BgwJob *job)
{
	/*
	 * Stub to allow the system to disable jobs based on the number of crashes
	 * or failures.
	 */
	return true;
}

TimestampTz
ts_bgw_job_stat_next_start(BgwJobStat *jobstat, BgwJob *job, int32 consecutive_failed_launches)
{
	/* give the system some room to breathe, wait before trying to launch again */
	if (consecutive_failed_launches > 0)
		return calculate_next_start_on_failed_launch(consecutive_failed_launches, job);
	if (jobstat == NULL)
		/* Never previously run - run right away */
		return DT_NOBEGIN;

	if (jobstat->fd.consecutive_crashes > 0)
	{
		/* Update the errors table regarding the crash */
		if (!ts_flags_are_set_32(jobstat->fd.flags, LAST_CRASH_REPORTED))
		{
			/* add the proc_schema, proc_name to the jsonb */
			NameData proc_schema = { .data = { 0 } }, proc_name = { .data = { 0 } };
			namestrcpy(&proc_schema, NameStr(job->fd.proc_schema));
			namestrcpy(&proc_name, NameStr(job->fd.proc_name));
			JsonbParseState *parse_state = NULL;
			pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
			ts_jsonb_add_str(parse_state, "proc_schema", NameStr(proc_schema));
			ts_jsonb_add_str(parse_state, "proc_name", NameStr(proc_name));
			JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

			const FormData_job_error jerr = {
				.error_data = JsonbValueToJsonb(result),
				.start_time = jobstat->fd.last_start,
				.finish_time = ts_timer_get_current_timestamp(),
				.pid = -1,
				.job_id = jobstat->fd.id,
			};

			ts_job_errors_insert_tuple(&jerr);
			ts_bgw_job_stat_mark_crash_reported(jobstat->fd.id);
		}

		return calculate_next_start_on_crash(jobstat->fd.consecutive_crashes, job);
	}

	return jobstat->fd.next_start;
}

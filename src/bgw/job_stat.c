/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <c.h>
#include <postgres.h>
#include <access/xact.h>

#include <math.h>

#include "job_stat.h"
#include "scanner.h"
#include "compat.h"
#include "timer.h"
#include "utils.h"

#if !PG96
#include <utils/fmgrprotos.h>
#endif
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

	*job_stat_pp = STRUCT_FROM_TUPLE(ti->tuple, ti->mctx, BgwJobStat, FormData_bgw_job_stat);

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
	ts_catalog_delete(ti->scanrel, ti->tuple);

	return SCAN_CONTINUE;
}

void
ts_bgw_job_stat_delete(int32 bgw_job_id)
{
	bgw_job_stat_scan_job_id(bgw_job_id, bgw_job_stat_tuple_delete, NULL, NULL, RowExclusiveLock);
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
	HeapTuple tuple = heap_copytuple(ti->tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(tuple);

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

	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	return SCAN_DONE;
}

typedef struct
{
	JobResult result;
	BgwJob *job;
} JobResultCtx;

static TimestampTz
calculate_next_start_on_success(TimestampTz finish_time, BgwJob *job)
{
	TimestampTz ts;
	TimestampTz last_finish = finish_time;
	if (!IS_VALID_TIMESTAMP(finish_time))
	{
		last_finish = ts_timer_get_current_timestamp();
	}
	ts = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
												 TimestampTzGetDatum(last_finish),
												 IntervalPGetDatum(&job->fd.schedule_interval)));
	return ts;
}

static float8
calculate_jitter_percent()
{
	/* returns a number in the range [-0.125, 0.125] */
	/* right now we use the postgres user-space RNG. if we become worried about
	 * correlated schedulers we can switch to
	 *     pg_strong_random(&percent, sizeof(percent));
	 * though we would need to figure out a way to make our tests pass
	 */
	uint8 percent = pg_lrand48();
	return ldexp((double) (16 - (int) (percent % 32)), -7);
}

/* For failures we have additive backoff based on consecutive failures
 * along with a ceiling at schedule_interval * MAX_INTERVALS_BACKOFF
 * We also limit the additive backoff in case of consecutive failures as we don't
 * want to pass in input that leads to out of range timestamps and don't want to
 * put off the next start time for the job indefinitely
 */
static TimestampTz
calculate_next_start_on_failure(TimestampTz finish_time, int consecutive_failures, BgwJob *job)
{
	float8 jitter = calculate_jitter_percent();
	/* consecutive failures includes this failure */
	TimestampTz res;
	volatile bool res_set = false;
	TimestampTz last_finish = finish_time;
	float8 multiplier = (consecutive_failures > MAX_FAILURES_MULTIPLIER ? MAX_FAILURES_MULTIPLIER :
																		  consecutive_failures);
	MemoryContext oldctx;
	if (!IS_VALID_TIMESTAMP(finish_time))
	{
		elog(LOG, "calculate_next_start_on_failure, got bad finish_time");
		last_finish = ts_timer_get_current_timestamp();
	}
	oldctx = CurrentMemoryContext;
	BeginInternalSubTransaction("next start on failure");
	PG_TRY();
	{
		/* ival = retry_period * (consecutive_failures)  */
		Datum ival = DirectFunctionCall2(interval_mul,
										 IntervalPGetDatum(&job->fd.retry_period),
										 Float8GetDatum(multiplier));

		/* ival_max is the ceiling = MAX_INTERVALS_BACKOFF * schedule_interval */
		Datum ival_max = DirectFunctionCall2(interval_mul,
											 IntervalPGetDatum(&job->fd.schedule_interval),
											 Float8GetDatum(MAX_INTERVALS_BACKOFF));

		if (DatumGetInt32(DirectFunctionCall2(interval_cmp, ival, ival_max)) > 0)
			ival = ival_max;

		/* Add some random jitter to prevent stampeding-herds, interval will be within about +-13%
		 */
		ival = DirectFunctionCall2(interval_mul, ival, Float8GetDatum(1.0 + jitter));

		res = DatumGetTimestampTz(
			DirectFunctionCall2(timestamptz_pl_interval, TimestampTzGetDatum(last_finish), ival));
		res_set = true;
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		ErrorData *errdata = CopyErrorData();
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("calculate_next_start_on_failure ran into an error, resetting value"),
				 errdetail("Error: %s", errdata->message)));
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldctx);
	if (!res_set)
	{
		TimestampTz nowt;
		/* job->fd_retry_period is a valid non-null value */
		nowt = ts_timer_get_current_timestamp();
		res = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
													  TimestampTzGetDatum(nowt),
													  IntervalPGetDatum(&job->fd.retry_period)));
	}
	return res;
}

/* For crashes, the logic is the similar as for failures except we also have
 *  a minimum wait after a crash that we wait, so that if an operator needs to disable the job,
 *  there will be enough time before another crash.
 */
static TimestampTz
calculate_next_start_on_crash(int consecutive_crashes, BgwJob *job)
{
	TimestampTz now = ts_timer_get_current_timestamp();
	TimestampTz failure_calc = calculate_next_start_on_failure(now, consecutive_crashes, job);
	TimestampTz min_time = TimestampTzPlusMilliseconds(now, MIN_WAIT_AFTER_CRASH_MS);

	if (min_time > failure_calc)
		return min_time;
	return failure_calc;
}

static ScanTupleResult
bgw_job_stat_tuple_mark_end(TupleInfo *ti, void *const data)
{
	JobResultCtx *result_ctx = data;
	HeapTuple tuple = heap_copytuple(ti->tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(tuple);
	Interval *duration;

	fd->last_finish = ts_timer_get_current_timestamp();

	duration = DatumGetIntervalP(DirectFunctionCall2(timestamp_mi,
													 TimestampTzGetDatum(fd->last_finish),
													 TimestampTzGetDatum(fd->last_start)));
	fd->total_duration =
		*DatumGetIntervalP(DirectFunctionCall2(interval_pl,
											   IntervalPGetDatum(&fd->total_duration),
											   IntervalPGetDatum(duration)));

	/* undo marking created by start marks */
	fd->last_run_success = result_ctx->result == JOB_SUCCESS ? true : false;
	fd->total_crashes--;
	fd->consecutive_crashes = 0;

	if (result_ctx->result == JOB_SUCCESS)
	{
		fd->total_success++;
		fd->consecutive_failures = 0;
		fd->last_successful_finish = fd->last_finish;
		/* Mark the next start at the end if the job itself hasn't */
		if (!bgw_job_stat_next_start_was_set(fd))
			fd->next_start = calculate_next_start_on_success(fd->last_finish, result_ctx->job);
	}
	else
	{
		fd->total_failures++;
		fd->consecutive_failures++;

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
															 result_ctx->job);
	}

	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	return SCAN_DONE;
}

static ScanTupleResult
bgw_job_stat_tuple_set_next_start(TupleInfo *ti, void *const data)
{
	TimestampTz *next_start = data;
	HeapTuple tuple = heap_copytuple(ti->tuple);
	FormData_bgw_job_stat *fd = (FormData_bgw_job_stat *) GETSTRUCT(tuple);

	fd->next_start = *next_start;

	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

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
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_success)] = Int64GetDatum(0);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_total_failures)] = Int64GetDatum(0);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_consecutive_failures)] = Int32GetDatum(0);

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
	/* Use double-check locking */
	if (!bgw_job_stat_scan_job_id(bgw_job_id,
								  bgw_job_stat_tuple_mark_start,
								  NULL,
								  NULL,
								  RowExclusiveLock))
	{
		Relation rel =
			table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT), ShareRowExclusiveLock);
		/* Recheck while having a self-exclusive lock */
		if (!bgw_job_stat_scan_job_id(bgw_job_id,
									  bgw_job_stat_tuple_mark_start,
									  NULL,
									  NULL,
									  RowExclusiveLock))
			bgw_job_stat_insert_relation(rel, bgw_job_id, true, DT_NOBEGIN);
		table_close(rel, ShareRowExclusiveLock);
	}
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
								  RowExclusiveLock))
		elog(ERROR, "unable to find job statistics for job %d", job->fd.id);
}

bool
ts_bgw_job_stat_end_was_marked(BgwJobStat *jobstat)
{
	return !TIMESTAMP_IS_NOBEGIN(jobstat->fd.last_finish);
}

TSDLLEXPORT void
ts_bgw_job_stat_set_next_start(BgwJob *job, TimestampTz next_start)
{
	/* Cannot use DT_NOBEGIN as that's the value used to indicate "not set" */
	if (next_start == DT_NOBEGIN)
		elog(ERROR, "cannot set next start to -infinity");

	if (!bgw_job_stat_scan_job_id(job->fd.id,
								  bgw_job_stat_tuple_set_next_start,
								  NULL,
								  &next_start,
								  RowExclusiveLock))
		elog(ERROR, "unable to find job statistics for job %d", job->fd.id);
}

/* update next_start if job stat exists */
TSDLLEXPORT bool
ts_bgw_job_stat_update_next_start(BgwJob *job, TimestampTz next_start, bool allow_unset)
{
	bool found = false;
	/* Cannot use DT_NOBEGIN as that's the value used to indicate "not set" */
	if (!allow_unset && next_start == DT_NOBEGIN)
		elog(ERROR, "cannot set next start to -infinity");

	found = bgw_job_stat_scan_job_id(job->fd.id,
									 bgw_job_stat_tuple_set_next_start,
									 NULL,
									 &next_start,
									 RowExclusiveLock);
	return found;
}

TSDLLEXPORT void
ts_bgw_job_stat_upsert_next_start(int32 bgw_job_id, TimestampTz next_start)
{
	/* Cannot use DT_NOBEGIN as that's the value used to indicate "not set" */
	if (next_start == DT_NOBEGIN)
		elog(ERROR, "cannot set next start to -infinity");

	/* Use double-check locking */
	if (!bgw_job_stat_scan_job_id(bgw_job_id,
								  bgw_job_stat_tuple_set_next_start,
								  NULL,
								  &next_start,
								  RowExclusiveLock))
	{
		Relation rel =
			table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT), ShareRowExclusiveLock);
		/* Recheck while having a self-exclusive lock */
		if (!bgw_job_stat_scan_job_id(bgw_job_id,
									  bgw_job_stat_tuple_set_next_start,
									  NULL,
									  &next_start,
									  RowExclusiveLock))
			bgw_job_stat_insert_relation(rel, bgw_job_id, true, next_start);
		table_close(rel, ShareRowExclusiveLock);
	}
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
ts_bgw_job_stat_next_start(BgwJobStat *jobstat, BgwJob *job)
{
	if (jobstat == NULL)
		/* Never previously run - run right away */
		return DT_NOBEGIN;

	if (jobstat->fd.consecutive_crashes > 0)
		return calculate_next_start_on_crash(jobstat->fd.consecutive_crashes, job);

	return jobstat->fd.next_start;
}

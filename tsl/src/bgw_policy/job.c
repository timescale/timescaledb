/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <continuous_agg.h>
#include <funcapi.h>
#include <hypertable_cache.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "bgw/timer.h"
#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw_policy/chunk_stats.h"
#include "bgw_policy/compression_api.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/retention_api.h"
#include "compression/compress_utils.h"
#include "continuous_aggs/materialize.h"
#include "continuous_aggs/job.h"
#include "tsl/src/chunk.h"

#include "config.h"
#include "errors.h"
#include "job.h"
#include "chunk.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "errors.h"
#include "job.h"
#include "license.h"
#include "reorder.h"
#include "utils.h"

#define ALTER_JOB_SCHEDULE_NUM_COLS 6
#define REORDER_SKIP_RECENT_DIM_SLICES_N 3

static void
enable_fast_restart(int32 job_id, const char *job_name)
{
	BgwJobStat *job_stat = ts_bgw_job_stat_find(job_id);

	ts_bgw_job_stat_set_next_start(job_id, job_stat->fd.last_start);
	elog(DEBUG1, "the %s job is scheduled to run again immediately", job_name);
}

/*
 * Returns the ID of a chunk to reorder. Eligible chunks must be at least the
 * 3rd newest chunk in the hypertable (not entirely exact because we use the number
 * of dimension slices as a proxy for the number of chunks),
 * not compressed, not dropped and hasn't been reordered recently.
 * For this version of automatic reordering, "not reordered
 * recently" means the chunk has not been reordered at all. This information
 * is available in the bgw_policy_chunk_stats metadata table.
 */
static int
get_chunk_id_to_reorder(int32 job_id, Hypertable *ht)
{
	Dimension *time_dimension = hyperspace_get_open_dimension(ht->space, 0);
	DimensionSlice *nth_dimension =
		ts_dimension_slice_nth_latest_slice(time_dimension->fd.id,
											REORDER_SKIP_RECENT_DIM_SLICES_N);

	if (!nth_dimension)
		return -1;

	Assert(time_dimension != NULL);

	return ts_dimension_slice_oldest_valid_chunk_for_reorder(job_id,
															 time_dimension->fd.id,
															 BTLessEqualStrategyNumber,
															 nth_dimension->fd.range_start,
															 InvalidStrategy,
															 -1);
}

static Datum
subtract_integer_from_now(int64 interval, Oid time_dim_type, Oid now_func)
{
	Datum now;
	int64 res;

	AssertArg(IS_INTEGER_TYPE(time_dim_type));

	now = OidFunctionCall0(now_func);

	switch (time_dim_type)
	{
		case INT2OID:
			res = DatumGetInt16(now) - interval;
			if (res < PG_INT16_MIN || res > PG_INT16_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW), errmsg("ts_interval overflow")));
			return Int16GetDatum(res);
		case INT4OID:
			res = DatumGetInt32(now) - interval;
			if (res < PG_INT32_MIN || res > PG_INT32_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW), errmsg("ts_interval overflow")));
			return Int32GetDatum(res);
		case INT8OID:
		{
			bool overflow = pg_sub_s64_overflow(DatumGetInt64(now), interval, &res);
			if (overflow)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW), errmsg("ts_interval overflow")));
			}
			return Int64GetDatum(res);
		}
		default:
			pg_unreachable();
	}
}

/*
 * returns now() - window as partitioning type datum
 */
static Datum
get_window_boundary(const Dimension *dim, const Jsonb *config, int64 (*int_getter)(const Jsonb *),
					Interval *(*interval_getter)(const Jsonb *) )
{
	Oid partitioning_type = ts_dimension_get_partition_type(dim);

	if (IS_INTEGER_TYPE(partitioning_type))
	{
		int64 lag = int_getter(config);
		Oid now_func = ts_get_integer_now_func(dim);

		Assert(now_func);

		return subtract_integer_from_now(lag, partitioning_type, now_func);
	}
	else
	{
		Datum res = TimestampTzGetDatum(GetCurrentTimestamp());
		Interval *lag = interval_getter(config);

		switch (partitioning_type)
		{
			case TIMESTAMPOID:
				res = DirectFunctionCall1(timestamptz_timestamp, res);
				res = DirectFunctionCall2(timestamp_mi_interval, res, IntervalPGetDatum(lag));

				return res;
			case TIMESTAMPTZOID:
				res = DirectFunctionCall2(timestamptz_mi_interval, res, IntervalPGetDatum(lag));

				return res;
			case DATEOID:
				res = DirectFunctionCall1(timestamptz_timestamp, res);
				res = DirectFunctionCall2(timestamp_mi_interval, res, IntervalPGetDatum(lag));
				res = DirectFunctionCall1(timestamp_date, res);

				return res;
			default:
				/* this should never happen as otherwise hypertable has unsupported time type */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unsupported time type %s", format_type_be(partitioning_type))));
				pg_unreachable();
		}
	}
}

static int32
get_chunk_to_compress(const Dimension *dim, const Jsonb *config)
{
	Oid partitioning_type = ts_dimension_get_partition_type(dim);
	StrategyNumber end_strategy = BTLessStrategyNumber;

	Datum boundary = get_window_boundary(dim,
										 config,
										 policy_compression_get_older_than_int,
										 policy_compression_get_older_than_interval);

	return ts_dimension_slice_get_chunkid_to_compress(dim->fd.id,
													  InvalidStrategy, /*start_strategy*/
													  -1,			   /*start_value*/
													  end_strategy,
													  ts_time_value_to_internal(boundary,
																				partitioning_type));
}

bool
policy_reorder_execute(int32 job_id, Jsonb *config, reorder_func reorder, bool fast_continue)
{
	int chunk_id;
	bool started = false;
	Hypertable *ht;
	Chunk *chunk;

	if (!IsTransactionOrTransactionBlock())
	{
		started = true;
		StartTransactionCommand();
	}

	ht = ts_hypertable_get_by_id(policy_reorder_get_hypertable_id(config));

	/* Find a chunk to reorder in the selected hypertable */
	chunk_id = get_chunk_id_to_reorder(job_id, ht);

	if (chunk_id == -1)
	{
		elog(NOTICE,
			 "no chunks need reordering for hypertable %s.%s",
			 ht->fd.schema_name.data,
			 ht->fd.table_name.data);
		goto commit;
	}

	/*
	 * NOTE: We pass the Oid of the hypertable's index, and the true reorder
	 * function should translate this to the Oid of the index on the specific
	 * chunk.
	 */
	chunk = ts_chunk_get_by_id(chunk_id, false);
	elog(LOG, "reordering chunk %s.%s", chunk->fd.schema_name.data, chunk->fd.table_name.data);
	reorder(chunk->table_id,
			get_relname_relid(policy_reorder_get_index_name(config),
							  get_namespace_oid(NameStr(ht->fd.schema_name), false)),
			false,
			InvalidOid,
			InvalidOid,
			InvalidOid);
	elog(LOG,
		 "completed reordering chunk %s.%s",
		 chunk->fd.schema_name.data,
		 chunk->fd.table_name.data);

	/* Now update chunk_stats table */
	ts_bgw_policy_chunk_stats_record_job_run(job_id, chunk_id, ts_timer_get_current_timestamp());

	if (fast_continue && get_chunk_id_to_reorder(job_id, ht) != -1)
		enable_fast_restart(job_id, "reorder");

commit:
	if (started)
		CommitTransactionCommand();
	elog(LOG, "job %d completed reordering", job_id);
	return true;
}

static Dimension *
get_open_dimension_for_hypertable(Hypertable *ht)
{
	int32 mat_id = ht->fd.id;
	Dimension *open_dim = hyperspace_get_open_dimension(ht->space, 0);
	Oid partitioning_type = ts_dimension_get_partition_type(open_dim);
	if (IS_INTEGER_TYPE(partitioning_type))
	{
		/* if this a materialization hypertable related to cont agg
		 * then need to get the right dimension which has
		 * integer_now function
		 */

		open_dim = ts_continuous_agg_find_integer_now_func_by_materialization_id(mat_id);
		if (open_dim == NULL)
		{
			elog(ERROR,
				 "missing integer_now function for hypertable \"%s\" ",
				 get_rel_name(ht->main_table_relid));
		}
	}
	return open_dim;
}

bool
policy_retention_execute(int32 job_id, Jsonb *config)
{
	bool started = false;
	Oid object_relid;
	Hypertable *hypertable;
	Cache *hcache;
	Dimension *open_dim;
	Datum boundary;
	Datum boundary_type;
	ContinuousAgg *cagg;

	if (!ActiveSnapshotSet())
	{
		started = true;
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	object_relid = ts_hypertable_id_to_relid(policy_retention_get_hypertable_id(config));
	hypertable = ts_hypertable_cache_get_cache_and_entry(object_relid, CACHE_FLAG_NONE, &hcache);
	open_dim = get_open_dimension_for_hypertable(hypertable);
	boundary = get_window_boundary(open_dim,
								   config,
								   policy_retention_get_retention_window_int,
								   policy_retention_get_retention_window_interval);
	boundary_type = ts_dimension_get_partition_type(open_dim);

	/* We need to do a reverse lookup here since the given hypertable
	   might be a materialized hypertable, and thus need to call drop_chunks
		on the continuous aggregate instead. */
	cagg = ts_continuous_agg_find_by_mat_hypertable_id(hypertable->fd.id);
	if (cagg)
	{
		const char *const view_name = NameStr(cagg->data.user_view_name);
		const char *const schema_name = NameStr(cagg->data.user_view_schema);
		object_relid = get_relname_relid(view_name, get_namespace_oid(schema_name, false));
	}

	chunk_invoke_drop_chunks(object_relid, boundary, boundary_type);

	ts_cache_release(hcache);

	if (started)
		PopActiveSnapshot();

	return true;
}

bool
policy_continuous_aggregate_execute(int32 job_id, Jsonb *config)
{
	bool committed = false;
	int32 materialization_id;
	bool finshed_all_materialization;
	ContinuousAggMatOptions mat_options;

	materialization_id = policy_continuous_aggregate_get_mat_hypertable_id(config);

	/* always materialize verbosely for now */
	mat_options = (ContinuousAggMatOptions){
		.verbose = true,
		.within_single_transaction = false,
		.process_only_invalidation = false,
		.invalidate_prior_to_time = PG_INT64_MAX,
	};

	/* materialization code has it's own transaction handling so we commit
	 * current transaction */
	if (IsTransactionOrTransactionBlock())
	{
		committed = true;
		CommitTransactionCommand();
	}

	finshed_all_materialization = continuous_agg_materialize(materialization_id, &mat_options);

	if (!finshed_all_materialization)
	{
		StartTransactionCommand();
		enable_fast_restart(job_id, "materialize continuous aggregate");
		CommitTransactionCommand();
	}

	/* start new transaction if we committed before */
	if (committed)
		StartTransactionCommand();

	return true;
}

bool
policy_compression_execute(int32 job_id, Jsonb *config)
{
	bool started = false;
	Oid table_relid;
	Hypertable *ht;
	Cache *hcache;
	int32 chunkid;
	Chunk *chunk = NULL;
	Dimension *dim;

	if (!ActiveSnapshotSet())
	{
		started = true;
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	table_relid = ts_hypertable_id_to_relid(policy_compression_get_hypertable_id(config));
	ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);
	dim = hyperspace_get_open_dimension(ht->space, 0);

	chunkid = get_chunk_to_compress(dim, config);
	if (chunkid == INVALID_CHUNK_ID)
	{
		elog(NOTICE,
			 "no chunks for hypertable %s.%s that satisfy compress chunk policy",
			 ht->fd.schema_name.data,
			 ht->fd.table_name.data);
	}
	else
	{
		chunk = ts_chunk_get_by_id(chunkid, true);
		tsl_compress_chunk_wrapper(chunk, false);

		elog(LOG,
			 "completed compressing chunk %s.%s",
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));
	}

	chunkid = get_chunk_to_compress(dim, config);
	if (chunkid != INVALID_CHUNK_ID)
		enable_fast_restart(job_id, "compression");

	ts_cache_release(hcache);
	if (started)
		PopActiveSnapshot();

	elog(DEBUG1, "job %d completed compressing chunk", job_id);
	return true;
}

static void
job_execute_function(FuncExpr *funcexpr)
{
	bool isnull;

	EState *estate = CreateExecutorState();
	ExprContext *econtext = CreateExprContext(estate);

	ExprState *es = ExecPrepareExpr((Expr *) funcexpr, estate);
	ExecEvalExpr(es, econtext, &isnull);

	FreeExprContext(econtext, true);
	FreeExecutorState(estate);
}

static void
job_execute_procedure(FuncExpr *funcexpr)
{
	CallStmt *call = makeNode(CallStmt);
	call->funcexpr = funcexpr;
	DestReceiver *dest = CreateDestReceiver(DestNone);
	/* we don't need to create proper param list cause we pass in all arguments as Const */
#ifdef PG11
	ParamListInfo params = palloc0(offsetof(ParamListInfoData, params));
#else
	ParamListInfo params = makeParamList(0);
#endif
	ExecuteCallStmt(call, params, false, dest);
}

bool
tsl_bgw_policy_job_execute(BgwJob *job)
{
	Const *arg1, *arg2;
	bool started;
	char prokind;
	Oid proc;
	Oid proc_args[] = { INT4OID, JSONBOID };
	List *name;
	FuncExpr *funcexpr;
	MemoryContext parent_ctx = CurrentMemoryContext;

	if (!IsTransactionOrTransactionBlock())
	{
		started = true;
		StartTransactionCommand();
	}

	name = list_make2(makeString(NameStr(job->fd.proc_schema)),
					  makeString(NameStr(job->fd.proc_name)));
	proc = LookupFuncName(name, 2, proc_args, false);

	prokind = get_func_prokind(proc);

	/*
	 * We need to switch back to parent MemoryContext as StartTransactionCommand
	 * switched to CurTransactionContext and this context will be destroyed
	 * on CommitTransactionCommand which may be too short-lived if a policy
	 * has its own transaction handling.
	 */
	MemoryContextSwitchTo(parent_ctx);
	arg1 = makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(job->fd.id), false, true);
	arg2 = makeConst(JSONBOID, -1, InvalidOid, -1, JsonbPGetDatum(job->fd.config), false, false);

	funcexpr = makeFuncExpr(proc,
							VOIDOID,
							list_make2(arg1, arg2),
							InvalidOid,
							InvalidOid,
							COERCE_EXPLICIT_CALL);

	switch (prokind)
	{
		case PROKIND_FUNCTION:
			job_execute_function(funcexpr);
			break;
		case PROKIND_PROCEDURE:
			job_execute_procedure(funcexpr);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported function type")));
			break;
	}

	if (started)
		CommitTransactionCommand();

	return true;
}

Datum
bgw_policy_alter_job_schedule(PG_FUNCTION_ARGS)
{
	BgwJob *job;
	BgwJobStat *stat;
	TupleDesc tupdesc;
	Datum values[ALTER_JOB_SCHEDULE_NUM_COLS];
	bool nulls[ALTER_JOB_SCHEDULE_NUM_COLS] = { false };
	HeapTuple tuple;
	TimestampTz next_start;

	int job_id = PG_GETARG_INT32(0);
	bool if_exists = PG_GETARG_BOOL(5);

	/* First get the job */
	job = ts_bgw_job_find(job_id, CurrentMemoryContext, false);

	if (!job)
	{
		if (if_exists)
		{
			ereport(NOTICE,
					(errmsg("cannot alter policy schedule, policy #%d not found, skipping",
							job_id)));
			PG_RETURN_NULL();
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("cannot alter policy schedule, policy #%d not found", job_id)));
	}

	ts_bgw_job_permission_check(job);

	if (!PG_ARGISNULL(1))
		job->fd.schedule_interval = *PG_GETARG_INTERVAL_P(1);
	if (!PG_ARGISNULL(2))
		job->fd.max_runtime = *PG_GETARG_INTERVAL_P(2);
	if (!PG_ARGISNULL(3))
		job->fd.max_retries = PG_GETARG_INT32(3);
	if (!PG_ARGISNULL(4))
		job->fd.retry_period = *PG_GETARG_INTERVAL_P(4);

	ts_bgw_job_update_by_id(job_id, job);

	if (!PG_ARGISNULL(6))
		ts_bgw_job_stat_upsert_next_start(job_id, PG_GETARG_TIMESTAMPTZ(6));

	/* Now look up the job and return it */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	stat = ts_bgw_job_stat_find(job_id);
	if (stat != NULL)
		next_start = stat->fd.next_start;
	else
		next_start = DT_NOBEGIN;

	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = Int32GetDatum(job->fd.id);
	values[1] = IntervalPGetDatum(&job->fd.schedule_interval);
	values[2] = IntervalPGetDatum(&job->fd.max_runtime);
	values[3] = Int32GetDatum(job->fd.max_retries);
	values[4] = IntervalPGetDatum(&job->fd.retry_period);
	values[5] = TimestampTzGetDatum(next_start);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	return HeapTupleGetDatum(tuple);
}

/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <funcapi.h>
#include <hypertable_cache.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <parser/parse_func.h>
#include <parser/parser.h>
#include <tcop/pquery.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/portal.h>
#include <utils/syscache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>
#include <extension.h>

#include "bgw/timer.h"
#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw_policy/chunk_stats.h"
#include "bgw_policy/compression_api.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/policy_utils.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/retention_api.h"
#include "compat/compat.h"
#include "compression/api.h"
#include "continuous_aggs/materialize.h"
#include "continuous_aggs/refresh.h"
#include "ts_catalog/continuous_agg.h"
#ifdef USE_TELEMETRY
#include "telemetry/telemetry.h"
#endif

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
#include "reorder.h"
#include "utils.h"

#define REORDER_SKIP_RECENT_DIM_SLICES_N 3

static void
log_retention_boundary(int elevel, PolicyRetentionData *policy_data, const char *message)
{
	char *relname;
	Datum boundary;
	Oid outfuncid = InvalidOid;
	bool isvarlena;

	getTypeOutputInfo(policy_data->boundary_type, &outfuncid, &isvarlena);

	relname = get_rel_name(policy_data->object_relid);
	boundary = policy_data->boundary;

	if (OidIsValid(outfuncid))
		elog(elevel,
			 "%s \"%s\": dropping data older than %s",
			 message,
			 relname,
			 DatumGetCString(OidFunctionCall1(outfuncid, boundary)));
}

static void
enable_fast_restart(int32 job_id, const char *job_name)
{
	BgwJobStat *job_stat = ts_bgw_job_stat_find(job_id);
	if (job_stat != NULL)
	{
		/* job might not have a valid last_start if it was not
		 * run by the bgw framework.
		 */
		ts_bgw_job_stat_set_next_start(job_id,
									   job_stat->fd.last_start != DT_NOBEGIN ?
										   job_stat->fd.last_start :
										   GetCurrentTransactionStartTimestamp());
	}
	else
		ts_bgw_job_stat_upsert_next_start(job_id, GetCurrentTransactionStartTimestamp());

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
	const Dimension *time_dimension = hyperspace_get_open_dimension(ht->space, 0);
	const DimensionSlice *nth_dimension =
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
		int64 res, lag = int_getter(config);
		Oid now_func = ts_get_integer_now_func(dim);

		Assert(now_func);

		res = ts_sub_integer_from_now(lag, partitioning_type, now_func);
		return Int64GetDatum(res);
	}
	else
	{
		Interval *lag = interval_getter(config);
		return subtract_interval_from_now(lag, partitioning_type);
	}
}

static List *
get_chunk_to_recompress(const Dimension *dim, const Jsonb *config)
{
	Oid partitioning_type = ts_dimension_get_partition_type(dim);
	StrategyNumber end_strategy = BTLessStrategyNumber;
	int32 numchunks = policy_compression_get_maxchunks_per_job(config);

	Datum boundary = get_window_boundary(dim,
										 config,
										 policy_recompression_get_recompress_after_int,
										 policy_recompression_get_recompress_after_interval);

	return ts_dimension_slice_get_chunkids_to_compress(dim->fd.id,
													   InvalidStrategy, /*start_strategy*/
													   -1,				/*start_value*/
													   end_strategy,
													   ts_time_value_to_internal(boundary,
																				 partitioning_type),
													   false,
													   true,
													   numchunks);
}

static void
check_valid_index(Hypertable *ht, const char *index_name)
{
	Oid index_oid;
	HeapTuple idxtuple;
	Form_pg_index index_form;

	index_oid = ts_get_relation_relid(NameStr(ht->fd.schema_name), (char *) index_name, true);
	idxtuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
	if (!HeapTupleIsValid(idxtuple))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("reorder index not found"),
				 errdetail("The index \"%s\" could not be found", index_name)));

	index_form = (Form_pg_index) GETSTRUCT(idxtuple);
	if (index_form->indrelid != ht->main_table_relid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid reorder index"),
				 errhint("The reorder index must by an index on hypertable \"%s\".",
						 NameStr(ht->fd.table_name))));

	ReleaseSysCache(idxtuple);
}

bool
policy_reorder_execute(int32 job_id, Jsonb *config)
{
	int chunk_id;
	Chunk *chunk;
	PolicyReorderData policy;

	policy_reorder_read_and_validate_config(config, &policy);

	/* Find a chunk to reorder in the selected hypertable */
	chunk_id = get_chunk_id_to_reorder(job_id, policy.hypertable);

	if (chunk_id == -1)
	{
		elog(NOTICE,
			 "no chunks need reordering for hypertable %s.%s",
			 policy.hypertable->fd.schema_name.data,
			 policy.hypertable->fd.table_name.data);
		return true;
	}

	/*
	 * NOTE: We pass the Oid of the hypertable's index, and the true reorder
	 * function should translate this to the Oid of the index on the specific
	 * chunk.
	 */
	chunk = ts_chunk_get_by_id(chunk_id, false);
	elog(DEBUG1, "reordering chunk %s.%s", chunk->fd.schema_name.data, chunk->fd.table_name.data);
	reorder_chunk(chunk->table_id, policy.index_relid, false, InvalidOid, InvalidOid, InvalidOid);
	elog(DEBUG1,
		 "completed reordering chunk %s.%s",
		 chunk->fd.schema_name.data,
		 chunk->fd.table_name.data);

	/* Now update chunk_stats table */
	ts_bgw_policy_chunk_stats_record_job_run(job_id, chunk_id, ts_timer_get_current_timestamp());

	if (get_chunk_id_to_reorder(job_id, policy.hypertable) != -1)
		enable_fast_restart(job_id, "reorder");

	return true;
}

void
policy_reorder_read_and_validate_config(Jsonb *config, PolicyReorderData *policy)
{
	int32 htid = policy_reorder_get_hypertable_id(config);
	Hypertable *ht = ts_hypertable_get_by_id(htid);

	if (!ht)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration hypertable id %d not found", htid)));

	const char *index_name = policy_reorder_get_index_name(config);
	check_valid_index(ht, index_name);

	if (policy)
	{
		policy->hypertable = ht;
		policy->index_relid =
			ts_get_relation_relid(NameStr(ht->fd.schema_name), (char *) index_name, false);
	}
}

bool
policy_retention_execute(int32 job_id, Jsonb *config)
{
	PolicyRetentionData policy_data;
	bool verbose_log;

	policy_retention_read_and_validate_config(config, &policy_data);

	verbose_log = policy_get_verbose_log(config);
	if (verbose_log)
		log_retention_boundary(LOG, &policy_data, "applying retention policy to hypertable");

	chunk_invoke_drop_chunks(policy_data.object_relid,
							 policy_data.boundary,
							 policy_data.boundary_type);

	return true;
}

void
policy_retention_read_and_validate_config(Jsonb *config, PolicyRetentionData *policy_data)
{
	Oid object_relid;
	Hypertable *hypertable;
	Cache *hcache;
	const Dimension *open_dim;
	Datum boundary;
	Datum boundary_type;
	ContinuousAgg *cagg;

	object_relid = ts_hypertable_id_to_relid(policy_retention_get_hypertable_id(config), false);
	hypertable = ts_hypertable_cache_get_cache_and_entry(object_relid, CACHE_FLAG_NONE, &hcache);
	open_dim = get_open_dimension_for_hypertable(hypertable);

	boundary = get_window_boundary(open_dim,
								   config,
								   policy_retention_get_drop_after_int,
								   policy_retention_get_drop_after_interval);
	boundary_type = ts_dimension_get_partition_type(open_dim);

	/* We need to do a reverse lookup here since the given hypertable might be
	   a materialized hypertable, and thus need to call drop_chunks on the
	   continuous aggregate instead. */
	cagg = ts_continuous_agg_find_by_mat_hypertable_id(hypertable->fd.id);
	if (cagg)
	{
		object_relid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
											 NameStr(cagg->data.user_view_name),
											 false);
	}

	ts_cache_release(hcache);

	if (policy_data)
	{
		policy_data->object_relid = object_relid;
		policy_data->boundary = boundary;
		policy_data->boundary_type = boundary_type;
	}
}

bool
policy_refresh_cagg_execute(int32 job_id, Jsonb *config)
{
	PolicyContinuousAggData policy_data;

	policy_refresh_cagg_read_and_validate_config(config, &policy_data);
	continuous_agg_refresh_internal(policy_data.cagg,
									&policy_data.refresh_window,
									CAGG_REFRESH_POLICY,
									policy_data.start_is_null,
									policy_data.end_is_null);

	return true;
}

void
policy_refresh_cagg_read_and_validate_config(Jsonb *config, PolicyContinuousAggData *policy_data)
{
	int32 materialization_id;
	Hypertable *mat_ht;
	const Dimension *open_dim;
	Oid dim_type;
	int64 refresh_start, refresh_end;
	bool start_isnull, end_isnull;

	materialization_id = policy_continuous_aggregate_get_mat_hypertable_id(config);
	mat_ht = ts_hypertable_get_by_id(materialization_id);

	if (!mat_ht)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration materialization hypertable id %d not found",
						materialization_id)));

	open_dim = get_open_dimension_for_hypertable(mat_ht);
	dim_type = ts_dimension_get_partition_type(open_dim);
	refresh_start = policy_refresh_cagg_get_refresh_start(open_dim, config, &start_isnull);
	refresh_end = policy_refresh_cagg_get_refresh_end(open_dim, config, &end_isnull);

	if (refresh_start >= refresh_end)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid refresh window"),
				 errdetail("start_offset: %s, end_offset: %s",
						   ts_internal_to_time_string(refresh_start, dim_type),
						   ts_internal_to_time_string(refresh_end, dim_type)),
				 errhint("The start of the window must be before the end.")));

	if (policy_data)
	{
		policy_data->refresh_window.type = dim_type;
		policy_data->refresh_window.start = refresh_start;
		policy_data->refresh_window.end = refresh_end;
		policy_data->cagg = ts_continuous_agg_find_by_mat_hypertable_id(materialization_id);
		policy_data->start_is_null = start_isnull;
		policy_data->end_is_null = end_isnull;
	}
}

/*
 * Invoke recompress_chunk via fmgr so that the call can be deparsed and sent to
 * remote data nodes.
 */
static void
policy_invoke_recompress_chunk(Chunk *chunk)
{
	EState *estate;
	ExprContext *econtext;
	FuncExpr *fexpr;
	Oid relid = chunk->table_id;
	Oid restype;
	Oid func_oid;
	List *args = NIL;
	bool isnull;
	Const *argarr[RECOMPRESS_CHUNK_NARGS] = {
		makeConst(REGCLASSOID,
				  -1,
				  InvalidOid,
				  sizeof(relid),
				  ObjectIdGetDatum(relid),
				  false,
				  false),
		castNode(Const, makeBoolConst(true, false)),
	};
	Oid type_id[RECOMPRESS_CHUNK_NARGS] = { REGCLASSOID, BOOLOID };
	char *schema_name = ts_extension_schema_name();
	List *fqn = list_make2(makeString(schema_name), makeString(RECOMPRESS_CHUNK_FUNCNAME));

	StaticAssertStmt(lengthof(type_id) == lengthof(argarr),
					 "argarr and type_id should have matching lengths");

	func_oid = LookupFuncName(fqn, lengthof(type_id), type_id, false);
	Assert(func_oid); /* LookupFuncName should not return an invalid OID */

	/* Prepare the function expr with argument list */
	get_func_result_type(func_oid, &restype, NULL);

	for (size_t i = 0; i < lengthof(argarr); i++)
		args = lappend(args, argarr[i]);

	fexpr = makeFuncExpr(func_oid, restype, args, InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	fexpr->funcretset = false;

	estate = CreateExecutorState();
	econtext = CreateExprContext(estate);

	ExprState *exprstate = ExecInitExpr(&fexpr->xpr, NULL);

	ExecEvalExprSwitchContext(exprstate, econtext, &isnull);

	/* Cleanup */
	FreeExprContext(econtext, false);
	FreeExecutorState(estate);
}

/* Read configuration for compression job from config object. */
void
policy_compression_read_and_validate_config(Jsonb *config, PolicyCompressionData *policy_data)
{
	Oid table_relid =
		ts_hypertable_id_to_relid(policy_compression_get_hypertable_id(config), false);
	Cache *hcache;
	Hypertable *hypertable =
		ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);
	if (policy_data)
	{
		policy_data->hypertable = hypertable;
		policy_data->hcache = hcache;
	}
}

void
policy_recompression_read_and_validate_config(Jsonb *config, PolicyCompressionData *policy_data)
{
	Oid table_relid =
		ts_hypertable_id_to_relid(policy_compression_get_hypertable_id(config), false);
	Cache *hcache;
	Hypertable *hypertable =
		ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);
	if (policy_data)
	{
		policy_data->hypertable = hypertable;
		policy_data->hcache = hcache;
	}
}

bool
policy_recompression_execute(int32 job_id, Jsonb *config)
{
	List *chunkid_lst;
	ListCell *lc;
	const Dimension *dim;
	PolicyCompressionData policy_data;
	bool distributed, used_portalcxt = false;
	MemoryContext saved_cxt, multitxn_cxt;

	policy_recompression_read_and_validate_config(config, &policy_data);
	dim = hyperspace_get_open_dimension(policy_data.hypertable->space, 0);
	distributed = hypertable_is_distributed(policy_data.hypertable);
	/* we want the chunk id list to survive across transactions. So alloc in
	 * a different context
	 */
	if (PortalContext)
	{
		/*if we have a portal context use that - it will get freed automatically*/
		multitxn_cxt = PortalContext;
		used_portalcxt = true;
	}
	else
	{
		/* background worker job does not go via usual CALL path, so we do
		 * not have a PortalContext */
		multitxn_cxt =
			AllocSetContextCreate(TopMemoryContext, "CompressionJobCxt", ALLOCSET_DEFAULT_SIZES);
	}
	saved_cxt = MemoryContextSwitchTo(multitxn_cxt);
	chunkid_lst = get_chunk_to_recompress(dim, config);
	MemoryContextSwitchTo(saved_cxt);

	if (!chunkid_lst)
	{
		elog(NOTICE,
			 "no chunks for hypertable \"%s.%s\" that satisfy recompress chunk policy",
			 policy_data.hypertable->fd.schema_name.data,
			 policy_data.hypertable->fd.table_name.data);
		ts_cache_release(policy_data.hcache);
		if (!used_portalcxt)
			MemoryContextDelete(multitxn_cxt);
		return true;
	}
	ts_cache_release(policy_data.hcache);
	if (ActiveSnapshotSet())
		PopActiveSnapshot();
	/* process each chunk in a new transaction */
	foreach (lc, chunkid_lst)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
		int32 chunkid = lfirst_int(lc);
		Chunk *chunk = ts_chunk_get_by_id(chunkid, true);
		if (!chunk || !ts_chunk_is_unordered(chunk))
			continue;
		if (distributed)
			policy_invoke_recompress_chunk(chunk);
		else
			tsl_recompress_chunk_wrapper(chunk);

		elog(LOG,
			 "completed recompressing chunk \"%s.%s\"",
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));
	}

	elog(DEBUG1, "job %d completed recompressing chunk", job_id);
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
	ParamListInfo params = makeParamList(0);
	ExecuteCallStmt(call, params, false, dest);
}

/*
 * Execute the job.
 *
 * This function can be called both from a portal and from a background
 * worker.
 */
bool
job_execute(BgwJob *job)
{
	Const *arg1, *arg2;
	bool portal_created = false;
	char prokind;
	Oid proc;
	ObjectWithArgs *object;
	FuncExpr *funcexpr;
	MemoryContext parent_ctx = CurrentMemoryContext;
	StringInfo query;
	Portal portal = ActivePortal;

	if (job->fd.config)
		elog(DEBUG1,
			 "Executing %s with parameters %s",
			 NameStr(job->fd.proc_name),
			 DatumGetCString(DirectFunctionCall1(jsonb_out, JsonbPGetDatum(job->fd.config))));
	else
		elog(DEBUG1, "Executing %s with no parameters", NameStr(job->fd.proc_name));
	/* Create a portal if there's no active */
	if (!PortalIsValid(portal))
	{
		portal_created = true;
		portal = CreatePortal("", true, true);
		portal->visible = false;
		portal->resowner = CurrentResourceOwner;
		ActivePortal = portal;
		PortalContext = portal->portalContext;

		StartTransactionCommand();
#if (PG13 && PG_VERSION_NUM >= 130004) || PG14_GE
		EnsurePortalSnapshotExists();
#else
		PushActiveSnapshot(GetTransactionSnapshot());
#endif
	}

#ifdef USE_TELEMETRY
	/* The telemetry job has a separate code path and since we can reach this
	 * code also when using run_job(), we have a special case here. This will
	 * not be triggered when executed from ts_bgw_job_execute(). */
	if (ts_is_telemetry_job(job))
	{
		/*
		 * In the first 12 hours, we want telemetry to ping every
		 * hour. After that initial period, we default to the
		 * schedule_interval listed in the job table.
		 */
		Interval one_hour = { .time = 1 * USECS_PER_HOUR };
		return ts_bgw_job_run_and_set_next_start(job,
												 ts_telemetry_main_wrapper,
												 TELEMETRY_INITIAL_NUM_RUNS,
												 &one_hour,
												 /* atomic */ false,
												 /* mark */ true);
	}
#endif

	object = makeNode(ObjectWithArgs);
	object->objname = list_make2(makeString(NameStr(job->fd.proc_schema)),
								 makeString(NameStr(job->fd.proc_name)));
	object->objargs = list_make2(SystemTypeName("int4"), SystemTypeName("jsonb"));
	proc = LookupFuncWithArgs(OBJECT_ROUTINE, object, false);

	prokind = get_func_prokind(proc);

	/*
	 * We need to switch back to parent MemoryContext as StartTransactionCommand
	 * switched to CurTransactionContext and this context will be destroyed
	 * on CommitTransactionCommand which may be too short-lived if a policy
	 * has its own transaction handling.
	 */
	MemoryContextSwitchTo(parent_ctx);
	arg1 = makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(job->fd.id), false, true);
	if (job->fd.config == NULL)
		arg2 = makeNullConst(JSONBOID, -1, InvalidOid);
	else
		arg2 =
			makeConst(JSONBOID, -1, InvalidOid, -1, JsonbPGetDatum(job->fd.config), false, false);

	funcexpr = makeFuncExpr(proc,
							VOIDOID,
							list_make2(arg1, arg2),
							InvalidOid,
							InvalidOid,
							COERCE_EXPLICIT_CALL);

	/* Here we create a query string from the function/procedure name that we
	 * are calling. We do not update the status after the execution has
	 * finished since this is wrapped inside the code that starts and stops
	 * any job, not just custom jobs. We just provide more detailed
	 * information here that we are actually calling a specific custom
	 * function. */
	query = makeStringInfo();
	appendStringInfo(query,
					 "CALL %s.%s()",
					 quote_identifier(NameStr(job->fd.proc_schema)),
					 quote_identifier(NameStr(job->fd.proc_name)));
	pgstat_report_activity(STATE_RUNNING, query->data);

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

	/* Drop portal if it was created */
	if (portal_created)
	{
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		CommitTransactionCommand();
		PortalDrop(portal, false);
		ActivePortal = NULL;
		PortalContext = NULL;
	}

	return true;
}

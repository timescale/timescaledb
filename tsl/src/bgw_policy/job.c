/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "cache.h"
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <extension.h>
#include <funcapi.h>
#include <hypertable_cache.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <parser/parse_func.h>
#include <parser/parser.h>
#include <tcop/pquery.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/portal.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>

#include "compat/compat.h"
#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw/timer.h"
#include "bgw_policy/chunk_stats.h"
#include "bgw_policy/compression_api.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/policy_config.h"
#include "bgw_policy/policy_utils.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/retention_api.h"
#include "compression/api.h"
#include "continuous_aggs/invalidation_threshold.h"
#include "continuous_aggs/materialize.h"
#include "continuous_aggs/refresh.h"
#include "ts_catalog/continuous_agg.h"
#ifdef USE_TELEMETRY
#include "telemetry/telemetry.h"
#endif

#include "tsl/src/chunk.h"

#include "chunk.h"
#include "config.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "guc.h"
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
			 "%s \"%s\": dropping data %s %s",
			 message,
			 relname,
			 policy_data->use_creation_time ? "created before" : "older than",
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
		Oid now_func = ts_get_integer_now_func(dim, false);

		/* If "now_func" is provided then we use that for calculating the window. */
		if (OidIsValid(now_func))
		{
			int64 res, lag = int_getter(config);
			res = ts_sub_integer_from_now(lag, partitioning_type, now_func);
			return Int64GetDatum(res);
		}
		else
		{
			/*
			 * Otherwise, the interval value can be returned without subtracting it
			 * from now().
			 */
			Interval *lag = interval_getter(config);
			return IntervalPGetDatum(lag);
		}
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
			 NameStr(policy.hypertable->fd.schema_name),
			 NameStr(policy.hypertable->fd.table_name));
		return true;
	}

	/*
	 * NOTE: We pass the Oid of the hypertable's index, and the true reorder
	 * function should translate this to the Oid of the index on the specific
	 * chunk.
	 */
	chunk = ts_chunk_get_by_id(chunk_id, false);
	elog(DEBUG1,
		 "reordering chunk %s.%s",
		 NameStr(chunk->fd.schema_name),
		 NameStr(chunk->fd.table_name));
	reorder_chunk(chunk->table_id, policy.index_relid, false, InvalidOid, InvalidOid, InvalidOid);
	elog(DEBUG1,
		 "completed reordering chunk %s.%s",
		 NameStr(chunk->fd.schema_name),
		 NameStr(chunk->fd.table_name));

	/* Now update chunk_stats table */
	ts_bgw_policy_chunk_stats_record_job_run(job_id, chunk_id, ts_timer_get_current_timestamp());

	if (get_chunk_id_to_reorder(job_id, policy.hypertable) != -1)
		enable_fast_restart(job_id, "reorder");

	return true;
}

void
policy_reorder_read_and_validate_config(Jsonb *config, PolicyReorderData *policy)
{
	int32 htid = policy_config_get_hypertable_id(config);
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
							 policy_data.boundary_type,
							 policy_data.use_creation_time);

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
	Interval *(*interval_getter)(const Jsonb *);
	interval_getter = policy_retention_get_drop_after_interval;
	bool use_creation_time = false;

	object_relid = ts_hypertable_id_to_relid(policy_config_get_hypertable_id(config), false);
	hypertable = ts_hypertable_cache_get_cache_and_entry(object_relid, CACHE_FLAG_NONE, &hcache);

	open_dim = get_open_dimension_for_hypertable(hypertable, false);

	/* if dim is NULL, then it should be an INTEGER partition with no int_now function */
	if (open_dim == NULL)
	{
		Oid partition_type;

		open_dim = hyperspace_get_open_dimension(hypertable->space, 0);
		partition_type = ts_dimension_get_partition_type(open_dim);
		if (!IS_INTEGER_TYPE(partition_type))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("incorrect partition type %d.  Expected integer", partition_type)));

		/* if there's no int_now function the boundary is considered as an INTERVAL */
		boundary_type = INTERVALOID;
		interval_getter = policy_retention_get_drop_created_before_interval;
		use_creation_time = true;
	}
	else
		boundary_type = ts_dimension_get_partition_type(open_dim);

	boundary =
		get_window_boundary(open_dim, config, policy_retention_get_drop_after_int, interval_getter);

	/* We need to do a reverse lookup here since the given hypertable might be
	   a materialized hypertable, and thus need to call drop_chunks on the
	   continuous aggregate instead. */
	cagg = ts_continuous_agg_find_by_mat_hypertable_id(hypertable->fd.id, true);
	if (cagg)
	{
		object_relid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
											 NameStr(cagg->data.user_view_name),
											 false);
	}

	ts_cache_release(&hcache);

	if (policy_data)
	{
		policy_data->object_relid = object_relid;
		policy_data->boundary = boundary;
		policy_data->boundary_type = boundary_type;
		policy_data->use_creation_time = use_creation_time;
	}
}

bool
policy_refresh_cagg_execute(int32 job_id, Jsonb *config)
{
	PolicyContinuousAggData policy_data;

	StringInfo str = makeStringInfo();
	JsonbToCStringIndent(str, &config->root, VARSIZE(config));

	policy_refresh_cagg_read_and_validate_config(config, &policy_data);

	bool enable_osm_reads_old = ts_guc_enable_osm_reads;

	if (!policy_data.include_tiered_data_isnull)
	{
		SetConfigOption("timescaledb.enable_tiered_reads",
						policy_data.include_tiered_data ? "on" : "off",
						PGC_USERSET,
						PGC_S_SESSION);
	}

	CaggRefreshContext context = { .callctx = CAGG_REFRESH_POLICY };

	/* Try to split window range into a list of ranges */
	List *refresh_window_list =
		continuous_agg_split_refresh_window(policy_data.cagg,
											&policy_data.refresh_window,
											policy_data.buckets_per_batch,
											policy_data.refresh_newest_first);
	if (refresh_window_list == NIL)
		refresh_window_list = lappend(refresh_window_list, &policy_data.refresh_window);
	else
		context.callctx = CAGG_REFRESH_POLICY_BATCHED;

	context.number_of_batches = list_length(refresh_window_list);

	ListCell *lc;
	int32 processing_batch = 0;
	foreach (lc, refresh_window_list)
	{
		InternalTimeRange *refresh_window = (InternalTimeRange *) lfirst(lc);
		elog(DEBUG1,
			 "refreshing continuous aggregate \"%s\" from %s to %s",
			 NameStr(policy_data.cagg->data.user_view_name),
			 ts_internal_to_time_string(refresh_window->start, refresh_window->type),
			 ts_internal_to_time_string(refresh_window->end, refresh_window->type));

		context.processing_batch = ++processing_batch;
		continuous_agg_refresh_internal(policy_data.cagg,
										refresh_window,
										context,
										refresh_window->start_isnull,
										refresh_window->end_isnull,
										false);
		if (processing_batch >= policy_data.max_batches_per_execution &&
			processing_batch < context.number_of_batches &&
			policy_data.max_batches_per_execution > 0)
		{
			elog(LOG,
				 "reached maximum number of batches per execution (%d), batches not processed (%d)",
				 policy_data.max_batches_per_execution,
				 context.number_of_batches - processing_batch);
			break;
		}
	}

	if (!policy_data.include_tiered_data_isnull)
	{
		SetConfigOption("timescaledb.enable_tiered_reads",
						enable_osm_reads_old ? "on" : "off",
						PGC_USERSET,
						PGC_S_SESSION);
	}

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
	int32 buckets_per_batch, max_batches_per_execution;
	bool start_isnull, end_isnull;
	bool include_tiered_data, include_tiered_data_isnull;
	bool refresh_newest_first;

	materialization_id = policy_continuous_aggregate_get_mat_hypertable_id(config);
	mat_ht = ts_hypertable_get_by_id(materialization_id);

	if (!mat_ht)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration materialization hypertable id %d not found",
						materialization_id)));

	ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(materialization_id, false);

	open_dim = get_open_dimension_for_hypertable(mat_ht, true);
	dim_type = ts_dimension_get_partition_type(open_dim);
	refresh_start = policy_refresh_cagg_get_refresh_start(cagg, open_dim, config, &start_isnull);
	refresh_end = policy_refresh_cagg_get_refresh_end(open_dim, config, &end_isnull);

	if (refresh_start >= refresh_end)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid refresh window"),
				 errdetail("start_offset: %s, end_offset: %s",
						   ts_internal_to_time_string(refresh_start, dim_type),
						   ts_internal_to_time_string(refresh_end, dim_type)),
				 errhint("The start of the window must be before the end.")));

	include_tiered_data =
		policy_refresh_cagg_get_include_tiered_data(config, &include_tiered_data_isnull);

	buckets_per_batch = policy_refresh_cagg_get_buckets_per_batch(config);

	if (buckets_per_batch < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid buckets per batch"),
				 errdetail("buckets_per_batch: %d", buckets_per_batch),
				 errhint("The buckets per batch should be greater than or equal to zero.")));

	max_batches_per_execution = policy_refresh_cagg_get_max_batches_per_execution(config);

	if (max_batches_per_execution < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid max batches per execution"),
				 errdetail("max_batches_per_execution: %d", max_batches_per_execution),
				 errhint(
					 "The max batches per execution should be greater than or equal to zero.")));

	refresh_newest_first = policy_refresh_cagg_get_refresh_newest_first(config);

	if (policy_data)
	{
		policy_data->refresh_window.type = dim_type;
		policy_data->refresh_window.start = refresh_start;
		policy_data->refresh_window.start_isnull = start_isnull;
		policy_data->refresh_window.end = refresh_end;
		policy_data->refresh_window.end_isnull = end_isnull;
		policy_data->cagg = cagg;
		policy_data->include_tiered_data = include_tiered_data;
		policy_data->include_tiered_data_isnull = include_tiered_data_isnull;
		policy_data->buckets_per_batch = buckets_per_batch;
		policy_data->max_batches_per_execution = max_batches_per_execution;
		policy_data->refresh_newest_first = refresh_newest_first;
	}
}

/* Read configuration for compression job from config object. */
void
policy_compression_read_and_validate_config(Jsonb *config, PolicyCompressionData *policy_data)
{
	Oid table_relid = ts_hypertable_id_to_relid(policy_config_get_hypertable_id(config), false);
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
	Oid table_relid = ts_hypertable_id_to_relid(policy_config_get_hypertable_id(config), false);
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
	bool used_portalcxt = false;
	MemoryContext saved_cxt, multitxn_cxt;

	policy_recompression_read_and_validate_config(config, &policy_data);
	dim = hyperspace_get_open_dimension(policy_data.hypertable->space, 0);
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
			 NameStr(policy_data.hypertable->fd.schema_name),
			 NameStr(policy_data.hypertable->fd.table_name));
		ts_cache_release(&policy_data.hcache);
		if (!used_portalcxt)
			MemoryContextDelete(multitxn_cxt);
		return true;
	}
	ts_cache_release(&policy_data.hcache);
	if (ActiveSnapshotSet())
		PopActiveSnapshot();
	/* process each chunk in a new transaction */
	foreach (lc, chunkid_lst)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
		int32 chunkid = lfirst_int(lc);
		Chunk *chunk = ts_chunk_get_by_id(chunkid, true);
		Assert(chunk);
		if (!ts_chunk_needs_recompression(chunk))
			continue;

		tsl_compress_chunk_wrapper(chunk, true, false);

		elog(LOG,
			 "completed recompressing chunk \"%s.%s\"",
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));
	}

	elog(DEBUG1, "job %d completed recompressing chunk", job_id);
	return true;
}

void
policy_process_hyper_inval_read_and_validate_config(Jsonb *config,
													PolicyMoveHyperInvalData *policy_data)
{
	int32 hypertable_id = policy_config_get_hypertable_id(config);
	Oid table_relid = ts_hypertable_id_to_relid(hypertable_id, true);

	if (!OidIsValid(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration hypertable id %d not found", hypertable_id)));

	Cache *hcache;
	Hypertable *hypertable =
		ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);
	if (policy_data)
	{
		policy_data->hypertable = hypertable;
		policy_data->hcache = hcache;
	}
	else
	{
		ts_cache_release(&hcache);
	}
}

bool
policy_process_hyper_inval_execute(int32 job_id, Jsonb *config)
{
	PolicyMoveHyperInvalData policy_data;

	policy_process_hyper_inval_read_and_validate_config(config, &policy_data);

	const Dimension *dim = hyperspace_get_open_dimension(policy_data.hypertable->space, 0);
	Oid dimtype = ts_dimension_get_partition_type(dim);
	int32 hypertable_id = policy_data.hypertable->fd.id;

	/* We serialized on the invalidation threshold, so we get and lock it. */
	invalidation_threshold_get(hypertable_id, dimtype);
	invalidation_process_hypertable_log(hypertable_id, dimtype);
	ts_cache_release(&policy_data.hcache);

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
		EnsurePortalSnapshotExists();
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

	proc = ts_bgw_job_get_funcid(job);
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

/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/tsmapi.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <commands/extension.h>
#include <executor/nodeAgg.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/plannodes.h>
#include <optimizer/appendinfo.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planner.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/elog.h>
#include <utils/fmgroids.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/selfuncs.h>
#include <utils/timestamp.h>

#include "compat/compat-msvc-enter.h"
#include <catalog/pg_constraint.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/cost.h>
#include <optimizer/plancat.h>
#include <parser/analyze.h>
#include <tcop/tcopprot.h>
#include "compat/compat-msvc-exit.h"

#include <math.h>

#include "annotations.h"
#include "chunk.h"
#include "cross_module_fn.h"
#include "debug_assert.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "extension.h"
#include "func_cache.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "import/allpaths.h"
#include "license_guc.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/chunk_dispatch/chunk_dispatch.h"
#include "nodes/constraint_aware_append/constraint_aware_append.h"
#include "nodes/hypertable_modify.h"
#include "partitioning.h"
#include "planner/planner.h"
#include "utils.h"

#include "compat/compat.h"
#include <common/hashfn.h>

#ifdef USE_TELEMETRY
#include "telemetry/functions.h"
#endif

/* define parameters necessary to generate the baserel info hash table interface */
typedef struct BaserelInfoEntry
{
	Oid reloid;
	Hypertable *ht;

	uint32 status; /* hash status */
} BaserelInfoEntry;

#define SH_PREFIX BaserelInfo
#define SH_ELEMENT_TYPE BaserelInfoEntry
#define SH_KEY_TYPE Oid
#define SH_KEY reloid
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_HASH_KEY(tb, key) murmurhash32(key)
#define SH_SCOPE static
#define SH_DECLARE
#define SH_DEFINE

// We don't need most of the generated functions and there is no way to not
// generate them.
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#endif

// Generate the baserel info hash table functions.
#include "lib/simplehash.h"
#ifdef __GNUC__

#pragma GCC diagnostic pop
#endif

void _planner_init(void);
void _planner_fini(void);

static planner_hook_type prev_planner_hook;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static set_join_pathlist_hook_type prev_set_join_pathlist_hook;
static get_relation_info_hook_type prev_get_relation_info_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;
static void cagg_reorder_groupby_clause(RangeTblEntry *subq_rte, Index rtno, List *outer_sortcl,
										List *outer_tlist);

/*
 * We mark range table entries (RTEs) in a query with TS_CTE_EXPAND if we'd like
 * to control table expansion ourselves. We exploit the ctename for this purpose
 * since it is not used for regular (base) relations.
 *
 * Note that we cannot use this mark as a general way to identify hypertable
 * RTEs. Child RTEs, for instance, will inherit this value from the parent RTE
 * during expansion. While we can prevent this happening in our custom table
 * expansion, we also have to account for the case when our custom expansion
 * is turned off with a GUC.
 */
static const char *TS_CTE_EXPAND = "ts_expand";

/*
 * Controls which type of fetcher to use to fetch data from the data nodes.
 * There is no place to store planner-global custom information (such as in
 * PlannerInfo). Because of this, we have to use the global variable that is
 * valid inside the scope of timescaledb_planner().
 * Note that that function can be called recursively, e.g. when evaluating a
 * SQL function at the planning time. We only have to determine the fetcher type
 * in the outermost scope, so we distinguish it by that the fetcher type is set
 * to the invalid value of 'auto'.
 */
DataFetcherType ts_data_node_fetcher_scan_type = AutoFetcherType;

/*
 * A simplehash hash table that records the chunks and their corresponding
 * hypertables, and also the plain baserels. We use it to tell whether a
 * relation is a hypertable chunk, inside the classify_relation function.
 * It is valid inside the scope of timescaledb_planner().
 * That function can be called recursively, e.g. when we evaluate a SQL function,
 * and this cache is initialized only at the top-level call.
 */
static struct BaserelInfo_hash *ts_baserel_info = NULL;

/*
 * Add information about a chunk to the baserel info cache. Used to cache the
 * chunk info at the plan time chunk exclusion.
 */
void
ts_add_baserel_cache_entry_for_chunk(Oid chunk_reloid, Hypertable *hypertable)
{
	Assert(hypertable != NULL);
	Assert(ts_baserel_info != NULL);

	bool found = false;
	BaserelInfoEntry *entry = BaserelInfo_insert(ts_baserel_info, chunk_reloid, &found);
	if (found)
	{
		/* Already cached. */
		Assert(entry->ht != NULL);
		return;
	}

	Assert(ts_chunk_get_hypertable_id_by_reloid(chunk_reloid) == hypertable->fd.id);

	/* Fill the cache entry. */
	entry->ht = hypertable;
}

static void
rte_mark_for_expansion(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(rte->ctename == NULL);
	rte->ctename = (char *) TS_CTE_EXPAND;
	rte->inh = false;
}

bool
ts_rte_is_marked_for_expansion(const RangeTblEntry *rte)
{
	if (NULL == rte->ctename)
		return false;

	if (rte->ctename == TS_CTE_EXPAND)
		return true;

	return strcmp(rte->ctename, TS_CTE_EXPAND) == 0;
}

/*
 * Planner-global hypertable cache.
 *
 * Each invocation of the planner (and our hooks) should reference the same
 * cache object. Since we warm the cache when pre-processing the query (prior to
 * invoking the planner), we'd like to ensure that we use the same cache object
 * throughout the planning of that query so that we can trust that the cache
 * holds the objects it was warmed with. Since the planner can be invoked
 * recursively, we also need to stack and pop cache objects.
 */
static List *planner_hcaches = NIL;

static Cache *
planner_hcache_push(void)
{
	Cache *hcache = ts_hypertable_cache_pin();

	planner_hcaches = lcons(hcache, planner_hcaches);

	return hcache;
}

static void
planner_hcache_pop(bool release)
{
	Cache *hcache;

	Assert(list_length(planner_hcaches) > 0);

	hcache = linitial(planner_hcaches);

	planner_hcaches = list_delete_first(planner_hcaches);

	if (release)
	{
		ts_cache_release(hcache);
		/* If we pop a stack and discover a new hypertable cache, the basrel
		 * cache can contain invalid entries, so we reset it. */
		if (planner_hcaches != NIL && hcache != linitial(planner_hcaches))
			BaserelInfo_reset(ts_baserel_info);
	}
}

static bool
planner_hcache_exists(void)
{
	return planner_hcaches != NIL;
}

static Cache *
planner_hcache_get(void)
{
	if (planner_hcaches == NIL)
		return NULL;

	return (Cache *) linitial(planner_hcaches);
}

/*
 * Get the Hypertable corresponding to the given relid.
 *
 * This function gets a hypertable from a pre-warmed hypertable cache. If
 * noresolve is specified (true), then it will do a cache-only lookup (i.e., it
 * will not try to scan metadata for a new entry to put in the cache). This
 * allows fast lookups during planning to also determine if something is _not_ a
 * hypertable.
 */
Hypertable *
ts_planner_get_hypertable(const Oid relid, const unsigned int flags)
{
	Cache *cache = planner_hcache_get();

	if (NULL == cache)
		return NULL;

	return ts_hypertable_cache_get_entry(cache, relid, flags);
}

bool
ts_rte_is_hypertable(const RangeTblEntry *rte, bool *isdistributed)
{
	Hypertable *ht = ts_planner_get_hypertable(rte->relid, CACHE_FLAG_CHECK);

	if (isdistributed && ht != NULL)
		*isdistributed = hypertable_is_distributed(ht);

	return ht != NULL;
}

#define IS_UPDL_CMD(parse)                                                                         \
	((parse)->commandType == CMD_UPDATE || (parse)->commandType == CMD_DELETE)

typedef struct
{
	Query *rootquery;
	Query *current_query;
	PlannerInfo *root;
	/*
	 * The number of distributed hypertables in the query and its subqueries.
	 * Specifically, we count range table entries here, so using the same
	 * distributed table twice counts as two tables. No matter whether it's the
	 * same physical table or not, the range table entries can be scanned
	 * concurrently, and more than one of them being distributed means we have
	 * to use the cursor fetcher so that these scans can be interleaved.
	 */
	int num_distributed_tables;
} PreprocessQueryContext;

/*
 * Preprocess the query tree, including, e.g., subqueries.
 *
 * Preprocessing includes:
 *
 * 1. Identifying all range table entries (RTEs) that reference
 *    hypertables. This will also warm the hypertable cache for faster lookup
 *    of both hypertables (cache hit) and non-hypertables (cache miss),
 *    without having to scan the metadata in either case.
 *
 * 2. Turning off inheritance for hypertable RTEs that we expand ourselves.
 *
 * 3. Reordering of GROUP BY clauses for continuous aggregates.
 *
 * 4. Constifying now() expressions for primary time dimension.
 */
static bool
preprocess_query(Node *node, PreprocessQueryContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FromExpr) && ts_guc_enable_optimizations)
	{
		FromExpr *from = castNode(FromExpr, node);
		if (from->quals)
		{
			if (ts_guc_enable_now_constify)
			{
				from->quals =
					ts_constify_now(context->root, context->current_query->rtable, from->quals);
			}
			/*
			 * We only amend space constraints for UPDATE/DELETE and SELECT FOR UPDATE
			 * as for normal SELECT we use our own hypertable expansion which can handle
			 * constraints on hashed space dimensions without further help.
			 */
			if (context->current_query->commandType != CMD_SELECT ||
				context->current_query->rowMarks != NIL)
			{
				from->quals = ts_add_space_constraints(context->root,
													   context->current_query->rtable,
													   from->quals);
			}
		}
	}

	else if (IsA(node, Query))
	{
		Query *query = castNode(Query, node);
		Query *prev_query;
		Cache *hcache = planner_hcache_get();
		ListCell *lc;
		Index rti = 1;
		bool ret;

		foreach (lc, query->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			Hypertable *ht;

			switch (rte->rtekind)
			{
				case RTE_SUBQUERY:
					if (ts_guc_enable_optimizations && ts_guc_enable_cagg_reorder_groupby &&
						query->commandType == CMD_SELECT)
					{
						/* applicable to selects on continuous aggregates */
						List *outer_tlist = query->targetList;
						List *outer_sortcl = query->sortClause;
						cagg_reorder_groupby_clause(rte, rti, outer_sortcl, outer_tlist);
					}
					break;
				case RTE_RELATION:
					/* This lookup will warm the cache with all hypertables in the query */
					ht = ts_hypertable_cache_get_entry(hcache, rte->relid, CACHE_FLAG_MISSING_OK);

					if (ht)
					{
						/* Mark hypertable RTEs we'd like to expand ourselves */
						if (ts_guc_enable_optimizations && ts_guc_enable_constraint_exclusion &&
							!IS_UPDL_CMD(context->rootquery) && query->resultRelation == 0 &&
							query->rowMarks == NIL && rte->inh)
							rte_mark_for_expansion(rte);

						if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
						{
							int compr_htid = ht->fd.compressed_hypertable_id;

							/* Also warm the cache with the compressed
							 * companion hypertable */
							ht = ts_hypertable_cache_get_entry_by_id(hcache, compr_htid);
							Assert(ht != NULL);
						}

						if (hypertable_is_distributed(ht))
						{
							context->num_distributed_tables++;
						}
					}
					else
					{
						/* To properly keep track of SELECT FROM ONLY <chunk> we
						 * have to mark the rte here because postgres will set
						 * rte->inh to false (when it detects the chunk has no
						 * children which is true for all our chunks) before it
						 * reaches set_rel_pathlist hook. But chunks from queries
						 * like SELECT ..  FROM ONLY <chunk> has rte->inh set to
						 * false and other chunks have rte->inh set to true.
						 * We want to distinguish between the two cases here by
						 * marking the chunk when rte->inh is true.
						 */
						Chunk *chunk = ts_chunk_get_by_relid(rte->relid, false);
						if (chunk && rte->inh)
							rte_mark_for_expansion(rte);
					}
					break;
				default:
					break;
			}
			rti++;
		}
		prev_query = context->current_query;
		context->current_query = query;
		ret = query_tree_walker(query, preprocess_query, context, 0);
		context->current_query = prev_query;
		return ret;
	}

	return expression_tree_walker(node, preprocess_query, context);
}

static PlannedStmt *
timescaledb_planner(Query *parse, const char *query_string, int cursor_opts,
					ParamListInfo bound_params)
{
	PlannedStmt *stmt;
	ListCell *lc;
	/*
	 * Volatile is needed because these are the local variables that are
	 * modified between setjmp/longjmp calls.
	 */
	volatile bool reset_fetcher_type = false;
	volatile bool reset_baserel_info = false;

	/*
	 * If we are in an aborted transaction, reject all queries.
	 * While this state will not happen during normal operation it
	 * can happen when executing plpgsql procedures.
	 */
	if (IsAbortedTransactionBlockState())
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	planner_hcache_push();
	if (ts_baserel_info == NULL)
	{
		/*
		 * The calls to timescaledb_planner can be recursive (e.g. when
		 * evaluating an immutable SQL function at planning time). We want to
		 * create and destroy the per-query baserel info table only at the
		 * top-level call, hence this flag.
		 */
		reset_baserel_info = true;

		/*
		 * This is a per-query cache, so we create it in the current memory
		 * context for the top-level call of this function, which hopefully
		 * should exist for the duration of the query. Message or portal
		 * memory contexts could also be suitable, but they don't exist for
		 * SPI calls.
		 */
		ts_baserel_info = BaserelInfo_create(CurrentMemoryContext,
											 /* nelements = */ 1,
											 /* private_data = */ NULL);
	}

	PG_TRY();
	{
		PreprocessQueryContext context = { 0 };
		PlannerGlobal glob = {
			.boundParams = bound_params,
		};
		PlannerInfo root = {
			.glob = &glob,
		};

		context.root = &root;
		context.rootquery = parse;
		context.current_query = parse;

		if (ts_extension_is_loaded())
		{
#ifdef USE_TELEMETRY
			ts_telemetry_function_info_gather(parse);
#endif
			/*
			 * Preprocess the hypertables in the query and warm up the caches.
			 */
			preprocess_query((Node *) parse, &context);

			/*
			 * Determine which type of fetcher to use. If set by GUC, use what
			 * is set. If the GUC says 'auto', use the COPY fetcher if we
			 * have at most one distributed table in the query. This enables
			 * parallel plans on data nodes, which speeds up the query.
			 * We can't use parallel plans with the cursor fetcher, because the
			 * cursors don't support parallel execution. This is because a
			 * cursor can be suspended at any time, then some arbitrary user
			 * code can be executed, and then the cursor is resumed. The
			 * parallel infrastructure doesn't have enough reentrability to
			 * survive this.
			 * We have to use a cursor fetcher when we have multiple distributed
			 * tables, because we might first have to get some rows from one
			 * table and then from another, without running either of them to
			 * completion first. This happens e.g. when doing a join. If we had
			 * a connection per table, we could avoid this requirement.
			 *
			 * Note that this function can be called recursively, e.g. when
			 * trying to evaluate an SQL function at the planning stage. We must
			 * only set/reset the fetcher type at the topmost level, that's why
			 * we check it's not already set.
			 */
			if (ts_data_node_fetcher_scan_type == AutoFetcherType)
			{
				reset_fetcher_type = true;

				if (context.num_distributed_tables >= 2)
				{
					if (ts_guc_remote_data_fetcher != CursorFetcherType &&
						ts_guc_remote_data_fetcher != AutoFetcherType)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("only cursor fetcher is supported for this query"),
								 errhint("COPY or prepared statement fetching of data is not "
										 "supported in "
										 "queries with multiple distributed hypertables."
										 " Use cursor fetcher instead.")));
					}
					ts_data_node_fetcher_scan_type = CursorFetcherType;
				}
				else
				{
					if (ts_guc_remote_data_fetcher == AutoFetcherType)
					{
						ts_data_node_fetcher_scan_type = CopyFetcherType;
					}
					else
					{
						ts_data_node_fetcher_scan_type = ts_guc_remote_data_fetcher;
					}
				}
			}

			Assert(ts_data_node_fetcher_scan_type != AutoFetcherType);
		}

		if (prev_planner_hook != NULL)
			/* Call any earlier hooks */
			stmt = (prev_planner_hook) (parse, query_string, cursor_opts, bound_params);
		else
			/* Call the standard planner */
			stmt = standard_planner(parse, query_string, cursor_opts, bound_params);

		if (ts_extension_is_loaded())
		{
			/*
			 * Our top-level HypertableInsert plan node that wraps ModifyTable needs
			 * to have a final target list that is the same as the ModifyTable plan
			 * node, and we only have access to its final target list after
			 * set_plan_references() (setrefs.c) has run at the end of
			 * standard_planner. Therefore, we fixup the final target list for
			 * HypertableInsert here.
			 */
			ts_hypertable_modify_fixup_tlist(stmt->planTree);

			foreach (lc, stmt->subplans)
			{
				Plan *subplan = (Plan *) lfirst(lc);

				if (subplan)
					ts_hypertable_modify_fixup_tlist(subplan);
			}

			if (IsA(stmt->planTree, Agg))
			{
				Agg *agg = castNode(Agg, stmt->planTree);

				/* If top-level plan is the finalize step of a partial
				 * aggregation, and it is wrapped in the partialize_agg()
				 * function, we want to do the combine step but skip
				 * finalization (e.g., for avg(), add up individual
				 * sum+counts, but don't compute the final average). */
				if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL &&
					has_partialize_function((Node *) agg->plan.targetlist, TS_FIX_AGGSPLIT_FINAL))
				{
					/* Deserialize input -> combine -> skip the final step ->
					 * serialize again */
					agg->aggsplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE |
									AGGSPLITOP_SERIALIZE | AGGSPLITOP_SKIPFINAL;
				}
			}
		}

		if (reset_baserel_info)
		{
			Assert(ts_baserel_info != NULL);
			BaserelInfo_destroy(ts_baserel_info);
			ts_baserel_info = NULL;
		}

		if (reset_fetcher_type)
		{
			ts_data_node_fetcher_scan_type = AutoFetcherType;
		}
	}
	PG_CATCH();
	{
		if (reset_baserel_info)
		{
			Assert(ts_baserel_info != NULL);
			BaserelInfo_destroy(ts_baserel_info);
			ts_baserel_info = NULL;
		}

		if (reset_fetcher_type)
		{
			ts_data_node_fetcher_scan_type = AutoFetcherType;
		}

		/* Pop the cache, but do not release since caches are auto-released on
		 * error */
		planner_hcache_pop(false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	planner_hcache_pop(true);

	return stmt;
}

static RangeTblEntry *
get_parent_rte(const PlannerInfo *root, Index rti)
{
	ListCell *lc;

	/* Fast path when arrays are setup */
	if (root->append_rel_array != NULL && root->append_rel_array[rti] != NULL)
	{
		AppendRelInfo *appinfo = root->append_rel_array[rti];
		return planner_rt_fetch(appinfo->parent_relid, root);
	}

	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);

		if (appinfo->child_relid == rti)
			return planner_rt_fetch(appinfo->parent_relid, root);
	}

	return NULL;
}

/*
 * Fetch cached baserel entry. If it does not exists, create an entry for this
 * relid.
 * If this relid corresponds to a chunk, cache additional chunk
 * related metadata: like chunk_status and pointer to hypertable entry.
 * It is okay to cache a pointer to the hypertable, since this cache is
 * confined to the lifetime of the query and not used across queries.
 * If the parent reolid is known, the caller can specify it to avoid the costly
 * lookup. Otherwise pass InvalidOid.
 */
static BaserelInfoEntry *
get_or_add_baserel_from_cache(Oid chunk_reloid, Oid parent_reloid)
{
	Hypertable *ht = NULL;
	/* First, check if this reloid is in cache. */
	bool found = false;
	BaserelInfoEntry *entry = BaserelInfo_insert(ts_baserel_info, chunk_reloid, &found);
	if (found)
	{
		return entry;
	}

	if (OidIsValid(parent_reloid))
	{
		ht = ts_planner_get_hypertable(parent_reloid, CACHE_FLAG_CHECK);

#ifdef USE_ASSERT_CHECKING
		/* Sanity check on the caller-specified hypertable reloid. */
		int32 parent_hypertable_id = ts_chunk_get_hypertable_id_by_reloid(chunk_reloid);
		if (parent_hypertable_id != INVALID_HYPERTABLE_ID)
		{
			Assert(ts_hypertable_id_to_relid(parent_hypertable_id, false) == parent_reloid);

			if (ht != NULL)
			{
				Assert(ht->fd.id == parent_hypertable_id);
			}
		}
#endif
	}
	else
	{
		/* Hypertable reloid not specified by the caller, look it up by
		 * an expensive metadata scan.
		 */
		int32 hypertable_id = ts_chunk_get_hypertable_id_by_reloid(chunk_reloid);

		if (hypertable_id != INVALID_HYPERTABLE_ID)
		{
			/* Hypertable reloid not specified by the caller, look it up. */
			parent_reloid = ts_hypertable_id_to_relid(hypertable_id, /* return_invalid */ false);

			ht = ts_planner_get_hypertable(parent_reloid, CACHE_FLAG_NONE);
			Assert(ht != NULL);
			Assert(ht->fd.id == hypertable_id);
		}
	}

	/* Cache the result. */
	entry->ht = ht;
	return entry;
}

/*
 * Classify a planned relation.
 *
 * This makes use of cache warming that happened during Query preprocessing in
 * the first planner hook.
 */
static TsRelType
classify_relation(const PlannerInfo *root, const RelOptInfo *rel, Hypertable **ht)
{
	Assert(ht != NULL);
	*ht = NULL;

	if (rel->reloptkind != RELOPT_BASEREL && rel->reloptkind != RELOPT_OTHER_MEMBER_REL)
	{
		return TS_REL_OTHER;
	}

	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

	if (!OidIsValid(rte->relid))
	{
		return TS_REL_OTHER;
	}

	if (rel->reloptkind == RELOPT_BASEREL)
	{
		/*
		 * To correctly classify relations in subqueries we cannot call
		 * ts_planner_get_hypertable with CACHE_FLAG_CHECK which includes
		 * CACHE_FLAG_NOCREATE flag because the rel might not be in cache yet.
		 */
		*ht = ts_planner_get_hypertable(rte->relid, CACHE_FLAG_MISSING_OK);

		if (*ht != NULL)
		{
			return TS_REL_HYPERTABLE;
		}

		/*
		 * This is either a chunk seen as a standalone table, or a non-chunk
		 * baserel. We need a costly chunk metadata scan to distinguish between
		 * them, so we cache the result of this lookup to avoid doing it
		 * repeatedly.
		 */
		BaserelInfoEntry *entry = get_or_add_baserel_from_cache(rte->relid, InvalidOid);
		*ht = entry->ht;
		return *ht ? TS_REL_CHUNK_STANDALONE : TS_REL_OTHER;
	}

	Assert(rel->reloptkind == RELOPT_OTHER_MEMBER_REL);

	RangeTblEntry *parent_rte = get_parent_rte(root, rel->relid);

	/*
	 * An entry of reloptkind RELOPT_OTHER_MEMBER_REL might still
	 * be a hypertable here if it was pulled up from a subquery
	 * as happens with UNION ALL for example. So we have to
	 * check for that to properly detect that pattern.
	 */
	if (parent_rte->rtekind == RTE_SUBQUERY)
	{
		*ht = ts_planner_get_hypertable(rte->relid,
										rte->inh ? CACHE_FLAG_MISSING_OK : CACHE_FLAG_CHECK);

		return *ht ? TS_REL_HYPERTABLE : TS_REL_OTHER;
	}

	if (parent_rte->relid == rte->relid)
	{
		/*
		 * A PostgreSQL table expansion peculiarity -- "self child", the root
		 * table that is expanded as a child of itself. This happens when our
		 * expansion code is turned off.
		 */
		*ht = ts_planner_get_hypertable(rte->relid, CACHE_FLAG_CHECK);
		return *ht != NULL ? TS_REL_HYPERTABLE_CHILD : TS_REL_OTHER;
	}

	/*
	 * Either an other baserel or a chunk seen when expanding the hypertable.
	 * Use the baserel cache to determine what it is.
	 */
	BaserelInfoEntry *entry = get_or_add_baserel_from_cache(rte->relid, parent_rte->relid);
	*ht = entry->ht;
	return *ht ? TS_REL_CHUNK_CHILD : TS_REL_OTHER;
}

extern void ts_sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel);

static inline bool
should_chunk_append(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel, Path *path, bool ordered,
					int order_attno)
{
	if (
#if PG14_LT
		root->parse->commandType != CMD_SELECT ||
#else
		/*
		 * We only support chunk exclusion on UPDATE/DELETE when no JOIN is involved on PG14+.
		 */
		((root->parse->commandType == CMD_DELETE || root->parse->commandType == CMD_UPDATE) &&
		 bms_num_members(root->all_baserels) > 1) ||
#endif
		!ts_guc_enable_chunk_append || hypertable_is_distributed(ht))
		return false;

	switch (nodeTag(path))
	{
		case T_AppendPath:
			/*
			 * If there are clauses that have mutable functions, or clauses that reference
			 * Params this Path might benefit from startup or runtime exclusion
			 */
			{
				AppendPath *append = castNode(AppendPath, path);
				ListCell *lc;

				/* Don't create ChunkAppend with no children */
				if (list_length(append->subpaths) == 0)
					return false;

				foreach (lc, rel->baserestrictinfo)
				{
					RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

					if (contain_mutable_functions((Node *) rinfo->clause) ||
						ts_contain_param((Node *) rinfo->clause))
						return true;
				}
				return false;
				break;
			}
		case T_MergeAppendPath:
			/*
			 * Can we do ordered append
			 */
			{
				MergeAppendPath *merge = castNode(MergeAppendPath, path);
				PathKey *pk;
				ListCell *lc;

				if (!ordered || path->pathkeys == NIL || list_length(merge->subpaths) == 0)
					return false;

				/* cannot support ordered append with OSM chunks. OSM chunk
				 * ranges are not recorded with the catalog
				 */
				if (ht && ts_chunk_get_osm_chunk_id(ht->fd.id) != INVALID_CHUNK_ID)
					return false;

				/*
				 * If we only have 1 child node there is no need for the
				 * ordered append optimization. We might still benefit from
				 * a ChunkAppend node here due to runtime chunk exclusion
				 * when we have non-immutable constraints.
				 */
				if (list_length(merge->subpaths) == 1)
				{
					foreach (lc, rel->baserestrictinfo)
					{
						RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

						if (contain_mutable_functions((Node *) rinfo->clause) ||
							ts_contain_param((Node *) rinfo->clause))
							return true;
					}
					return false;
				}

				pk = linitial_node(PathKey, path->pathkeys);

				/*
				 * Check PathKey is compatible with Ordered Append ordering
				 * we created when expanding hypertable.
				 * Even though ordered is true on the RelOptInfo we have to
				 * double check that current Path fulfills requirements for
				 * Ordered Append transformation because the RelOptInfo may
				 * be used for multiple Pathes.
				 */
				Expr *em_expr = find_em_expr_for_rel(pk->pk_eclass, rel);

				/*
				 * If this is a join the ordering information might not be
				 * for the current rel and have no EquivalenceMember.
				 */

				if (!em_expr)
					return false;

				if (IsA(em_expr, Var) && castNode(Var, em_expr)->varattno == order_attno)
					return true;
				else if (IsA(em_expr, FuncExpr) && list_length(path->pathkeys) == 1)
				{
					FuncExpr *func = castNode(FuncExpr, em_expr);
					FuncInfo *info = ts_func_cache_get_bucketing_func(func->funcid);
					Expr *transformed;

					if (info != NULL)
					{
						transformed = info->sort_transform(func);
						if (IsA(transformed, Var) &&
							castNode(Var, transformed)->varattno == order_attno)
							return true;
					}
				}

				return false;
				break;
			}
		default:
			return false;
	}
}

static inline bool
should_constraint_aware_append(PlannerInfo *root, Hypertable *ht, Path *path)
{
	/* Constraint-aware append currently expects children that scans a real
	 * "relation" (e.g., not an "upper" relation). So, we do not run it on a
	 * distributed hypertable because the append children are typically
	 * per-server relations without a corresponding "real" table in the
	 * system. Further, per-server appends shouldn't need runtime pruning in any
	 * case. */
	if (root->parse->commandType != CMD_SELECT || hypertable_is_distributed(ht))
		return false;

	return ts_constraint_aware_append_possible(path);
}

static bool
rte_should_expand(const RangeTblEntry *rte)
{
	bool is_hypertable = ts_rte_is_hypertable(rte, NULL);

	return is_hypertable && !rte->inh && ts_rte_is_marked_for_expansion(rte);
}

static void
reenable_inheritance(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	bool set_pathlist_for_current_rel = false;
	double total_pages;
	bool reenabled_inheritance = false;

	for (int i = 1; i < root->simple_rel_array_size; i++)
	{
		RangeTblEntry *in_rte = root->simple_rte_array[i];

		if (rte_should_expand(in_rte))
		{
			RelOptInfo *in_rel = root->simple_rel_array[i];
			Hypertable *ht = ts_planner_get_hypertable(in_rte->relid, CACHE_FLAG_NOCREATE);

			Assert(ht != NULL && in_rel != NULL);
			ts_plan_expand_hypertable_chunks(ht, root, in_rel);

			in_rte->inh = true;
			reenabled_inheritance = true;
			/* Redo set_rel_consider_parallel, as results of the call may no longer be valid
			 * here (due to adding more tables to the set of tables under consideration here).
			 * This is especially true if dealing with foreign data wrappers. */

			/*
			 * An entry of reloptkind RELOPT_OTHER_MEMBER_REL might still
			 * be a hypertable here if it was pulled up from a subquery
			 * as happens with UNION ALL for example.
			 */
			if (in_rel->reloptkind == RELOPT_BASEREL ||
				in_rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
			{
				Assert(in_rte->relkind == RELKIND_RELATION);
				ts_set_rel_size(root, in_rel, i, in_rte);
			}

			/* if we're activating inheritance during a hypertable's pathlist
			 * creation then we're past the point at which postgres will add
			 * paths for the children, and we have to do it ourselves. We delay
			 * the actual setting of the pathlists until after this loop,
			 * because set_append_rel_pathlist will eventually call this hook again.
			 */
			if (in_rte == rte)
			{
				Assert(rti == (Index) i);
				set_pathlist_for_current_rel = true;
			}
		}
	}

	if (!reenabled_inheritance)
		return;

	total_pages = 0;
	for (int i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *brel = root->simple_rel_array[i];

		if (brel == NULL)
			continue;

		Assert(brel->relid == (Index) i); /* sanity check on array */

		if (IS_DUMMY_REL(brel))
			continue;

		if (IS_SIMPLE_REL(brel))
			total_pages += (double) brel->pages;
	}
	root->total_table_pages = total_pages;

	if (set_pathlist_for_current_rel)
	{
		bool do_distributed;

		Hypertable *ht = ts_planner_get_hypertable(rte->relid, CACHE_FLAG_NOCREATE);
		Assert(ht != NULL);

		/* the hypertable will have been planned as if it was a regular table
		 * with no data. Since such a plan would be cheaper than any real plan,
		 * it would always be used, and we need to remove these plans before
		 * adding ours.
		 *
		 * Also, if it's a distributed hypertable and per data node queries are
		 * enabled then we will be throwing this below append path away. So only
		 * build it otherwise
		 */
		do_distributed = !IS_DUMMY_REL(rel) && hypertable_is_distributed(ht) &&
						 ts_guc_enable_per_data_node_queries;

		rel->pathlist = NIL;
		rel->partial_pathlist = NIL;
		/* allow a session parameter to override the use of this datanode only path */
#ifdef TS_DEBUG
		if (do_distributed)
		{
			const char *allow_dn_path =
				GetConfigOption("timescaledb.debug_allow_datanode_only_path", true, false);
			if (allow_dn_path && pg_strcasecmp(allow_dn_path, "on") != 0)
			{
				do_distributed = false;
				elog(DEBUG2, "creating per chunk append paths");
			}
			else
				elog(DEBUG2, "avoiding per chunk append paths");
		}
#endif

		if (!do_distributed)
			ts_set_append_rel_pathlist(root, rel, rti, rte);
	}
}

static void
apply_optimizations(PlannerInfo *root, TsRelType reltype, RelOptInfo *rel, RangeTblEntry *rte,
					Hypertable *ht)
{
	if (!ts_guc_enable_optimizations)
		return;

	switch (reltype)
	{
		case TS_REL_HYPERTABLE_CHILD:
			/* empty table so nothing to optimize */
			break;
		case TS_REL_CHUNK_STANDALONE:
		case TS_REL_CHUNK_CHILD:
			ts_sort_transform_optimization(root, rel);
			/*
			 * Since the sort optimization adds new paths to the rel it has
			 * to happen before any optimizations that replace pathlist.
			 */
			if (ts_cm_functions->set_rel_pathlist_query != NULL)
				ts_cm_functions->set_rel_pathlist_query(root, rel, rel->relid, rte, ht);
			break;
		default:
			break;
	}

	if (reltype == TS_REL_HYPERTABLE &&
#if PG14_GE
		(root->parse->commandType == CMD_SELECT || root->parse->commandType == CMD_DELETE ||
		 root->parse->commandType == CMD_UPDATE)
#else
		/*
		 * For PG < 14 commandType will be CMD_SELECT even when planning DELETE so we
		 * check resultRelation instead.
		 */
		root->parse->resultRelation == 0
#endif
	)
	{
		TimescaleDBPrivate *private = ts_get_private_reloptinfo(rel);
		bool ordered = private->appends_ordered;
		int order_attno = private->order_attno;
		List *nested_oids = private->nested_oids;
		ListCell *lc;

		Assert(ht != NULL);

		foreach (lc, rel->pathlist)
		{
			Path **pathptr = (Path **) &lfirst(lc);

			switch (nodeTag(*pathptr))
			{
				case T_AppendPath:
				case T_MergeAppendPath:
					if (should_chunk_append(ht, root, rel, *pathptr, ordered, order_attno))
						*pathptr = ts_chunk_append_path_create(root,
															   rel,
															   ht,
															   *pathptr,
															   false,
															   ordered,
															   nested_oids);
					else if (should_constraint_aware_append(root, ht, *pathptr))
						*pathptr = ts_constraint_aware_append_path_create(root, *pathptr);
					break;
				default:
					break;
			}
		}

		foreach (lc, rel->partial_pathlist)
		{
			Path **pathptr = (Path **) &lfirst(lc);

			switch (nodeTag(*pathptr))
			{
				case T_AppendPath:
				case T_MergeAppendPath:
					if (should_chunk_append(ht, root, rel, *pathptr, false, 0))
						*pathptr =
							ts_chunk_append_path_create(root, rel, ht, *pathptr, true, false, NIL);
					else if (should_constraint_aware_append(root, ht, *pathptr))
						*pathptr = ts_constraint_aware_append_path_create(root, *pathptr);
					break;
				default:
					break;
			}
		}
	}
}

static bool
valid_hook_call(void)
{
	return ts_extension_is_loaded() && planner_hcache_exists();
}

static bool
dml_involves_hypertable(PlannerInfo *root, Hypertable *ht, Index rti)
{
	Index result_rti = root->parse->resultRelation;
	RangeTblEntry *result_rte = planner_rt_fetch(result_rti, root);

	return result_rti == rti || ht->main_table_relid == result_rte->relid;
}

static void
timescaledb_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	TsRelType reltype;
	Hypertable *ht;

	/* Quick exit if this is a relation we're not interested in */
	if (!valid_hook_call() || !OidIsValid(rte->relid) || IS_DUMMY_REL(rel))
	{
		if (prev_set_rel_pathlist_hook != NULL)
			(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);
		return;
	}

	reltype = classify_relation(root, rel, &ht);

	/* Check for unexpanded hypertable */
	if (!rte->inh && ts_rte_is_marked_for_expansion(rte))
		reenable_inheritance(root, rel, rti, rte);

	if (ts_guc_enable_optimizations)
		ts_planner_constraint_cleanup(root, rel);

	/* Call other extensions. Do it after table expansion. */
	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);

	if (ts_cm_functions->set_rel_pathlist != NULL)
		ts_cm_functions->set_rel_pathlist(root, rel, rti, rte);

	switch (reltype)
	{
		case TS_REL_HYPERTABLE_CHILD:
			if (ts_guc_enable_optimizations && IS_UPDL_CMD(root->parse))
				ts_planner_constraint_cleanup(root, rel);

			break;
		case TS_REL_CHUNK_STANDALONE:
		case TS_REL_CHUNK_CHILD:
			/* Check for UPDATE/DELETE/MERGE (DML) on compressed chunks */
			if (IS_UPDL_CMD(root->parse) && dml_involves_hypertable(root, ht, rti))
			{
				if (ts_cm_functions->set_rel_pathlist_dml != NULL)
					ts_cm_functions->set_rel_pathlist_dml(root, rel, rti, rte, ht);
				break;
			}
#if PG15_GE
			/*
			 * For MERGE command if there is an UPDATE or DELETE action, then
			 * do not allow this to succeed on compressed chunks
			 */
			if (root->parse->commandType == CMD_MERGE && dml_involves_hypertable(root, ht, rti))
			{
				ListCell *ml;
				foreach (ml, root->parse->mergeActionList)
				{
					MergeAction *action = (MergeAction *) lfirst(ml);
					if (action->commandType == CMD_UPDATE || action->commandType == CMD_DELETE)
					{
						if (ts_cm_functions->set_rel_pathlist_dml != NULL)
							ts_cm_functions->set_rel_pathlist_dml(root, rel, rti, rte, ht);
					}
				}
				break;
			}
#endif
			TS_FALLTHROUGH;
		default:
			apply_optimizations(root, reltype, rel, rte, ht);
			break;
	}
}

/* This hook is meant to editorialize about the information the planner gets
 * about a relation. We use it to attach our own metadata to hypertable and
 * chunk relations that we need during planning. We also expand hypertables
 * here. */
static void
timescaledb_get_relation_info_hook(PlannerInfo *root, Oid relation_objectid, bool inhparent,
								   RelOptInfo *rel)
{
	Hypertable *ht;

	if (prev_get_relation_info_hook != NULL)
		prev_get_relation_info_hook(root, relation_objectid, inhparent, rel);

	if (!valid_hook_call())
		return;

	switch (classify_relation(root, rel, &ht))
	{
		case TS_REL_HYPERTABLE:
		{
			RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
			Query *query = root->parse;
			/* Mark hypertable RTEs we'd like to expand ourselves.
			 * Hypertables inside inlineable functions don't get marked during the query
			 * preprocessing step. Therefore we do an extra try here. However, we need to
			 * be careful for UPDATE/DELETE as Postgres (in at least version 12) plans them
			 * in a complicated way (see planner.c:inheritance_planner). First, it runs the
			 * UPDATE/DELETE through the planner as a simulated SELECT. It uses the results
			 * of this fake planning to adapt its own UPDATE/DELETE plan. Then it's planned
			 * a second time as a real UPDATE/DELETE, but with requiredPerms set to 0, as it
			 * assumes permission checking has been done already during the first planner call.
			 * We don't want to touch the UPDATE/DELETEs, so we need to check all the regular
			 * conditions here that are checked during preprocess_query, as well as the
			 * condition that rte->requiredPerms is not requiring UPDATE/DELETE on this rel.
			 */
			if (ts_guc_enable_optimizations && ts_guc_enable_constraint_exclusion && inhparent &&
				rte->ctename == NULL && !IS_UPDL_CMD(query) && query->resultRelation == 0 &&
				query->rowMarks == NIL && (rte->requiredPerms & (ACL_UPDATE | ACL_DELETE)) == 0)
			{
				rte_mark_for_expansion(rte);
			}
			ts_create_private_reloptinfo(rel);
			ts_plan_expand_timebucket_annotate(root, rel);
			break;
		}
		case TS_REL_CHUNK_STANDALONE:
		case TS_REL_CHUNK_CHILD:
		{
			ts_create_private_reloptinfo(rel);

			if (ts_guc_enable_transparent_decompression && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
			{
				RangeTblEntry *chunk_rte = planner_rt_fetch(rel->relid, root);
				Chunk *chunk = ts_chunk_get_by_relid(chunk_rte->relid, true);

				if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
				{
					/* Planning indexes is expensive, and if this is a fully compressed chunk, we
					 * know we'll never need to use indexes on the uncompressed version, since
					 * all the data is in the compressed chunk anyway. Therefore, it is much
					 * faster if we simply trash the indexlist here and never plan any useless
					 * IndexPaths at all.
					 * If the chunk is partially compressed, then we should enable indexScan
					 * on the uncompressed part.
					 */
					if (!ts_chunk_is_partial(chunk))
						rel->indexlist = NIL;
				}
			}
			break;
		}
		case TS_REL_HYPERTABLE_CHILD:
			/* When postgres expands an inheritance tree it also adds the
			 * parent hypertable as child relation. Since for a hypertable the
			 * parent will never have any data we can mark this relation as
			 * dummy relation so it gets ignored in later steps. This is only
			 * relevant for code paths that use the postgres inheritance code
			 * as we don't include the hypertable as child when expanding the
			 * hypertable ourself.
			 * We do exclude distributed hypertables for now to not alter
			 * the trigger behaviour on access nodes, which would otherwise
			 * no longer fire.
			 */
			if (IS_UPDL_CMD(root->parse) && !hypertable_is_distributed(ht))
				mark_dummy_rel(rel);
			break;
		case TS_REL_OTHER:
			break;
	}
}

static bool
join_involves_hypertable(const PlannerInfo *root, const RelOptInfo *rel)
{
	int relid = -1;

	while ((relid = bms_next_member(rel->relids, relid)) >= 0)
	{
		const RangeTblEntry *rte = planner_rt_fetch(relid, root);

		if (rte != NULL)
			/* This might give a false positive for chunks in case of PostgreSQL
			 * expansion since the ctename is copied from the parent hypertable
			 * to the chunk */
			return ts_rte_is_marked_for_expansion(rte);
	}
	return false;
}

static bool
involves_hypertable(PlannerInfo *root, RelOptInfo *rel)
{
	if (rel->reloptkind == RELOPT_JOINREL)
		return join_involves_hypertable(root, rel);

	Hypertable *ht;
	return classify_relation(root, rel, &ht) == TS_REL_HYPERTABLE;
}

/*
 * Replace INSERT (ModifyTablePath) paths on hypertables.
 *
 * From the ModifyTable description: "Each ModifyTable node contains
 * a list of one or more subplans, much like an Append node.  There
 * is one subplan per result relation."
 *
 * The subplans produce the tuples for INSERT, while the result relation is the
 * table we'd like to insert into.
 *
 * The way we redirect tuples to chunks is to insert an intermediate "chunk
 * dispatch" plan node, between the ModifyTable and its subplan that produces
 * the tuples. When the ModifyTable plan is executed, it tries to read a tuple
 * from the intermediate chunk dispatch plan instead of the original
 * subplan. The chunk plan reads the tuple from the original subplan, looks up
 * the chunk, sets the executor's resultRelation to the chunk table and finally
 * returns the tuple to the ModifyTable node.
 *
 * We also need to wrap the ModifyTable plan node with a HypertableInsert node
 * to give the ChunkDispatchState node access to the ModifyTableState node in
 * the execution phase.
 *
 * Conceptually, the plan modification looks like this:
 *
 * Original plan:
 *
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 *
 * Modified plan:
 *
 *	[ HypertableModify ]
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^			   ^
 *		  | Tuple	  / <Set resultRelation to the matching chunk table>
 *		  |			 /
 * [ ChunkDispatch ]
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 * For PG < 14, the modifytable plan is modified for INSERTs only.
 * For PG14+, we modify the plan for DELETEs as well.
 *
 */
static List *
replace_hypertable_modify_paths(PlannerInfo *root, List *pathlist, RelOptInfo *input_rel)
{
	List *new_pathlist = NIL;
	ListCell *lc;

	foreach (lc, pathlist)
	{
		Path *path = lfirst(lc);

		if (IsA(path, ModifyTablePath))
		{
			ModifyTablePath *mt = castNode(ModifyTablePath, path);
			RangeTblEntry *rte = planner_rt_fetch(mt->nominalRelation, root);
			Hypertable *ht = ts_planner_get_hypertable(rte->relid, CACHE_FLAG_CHECK);
			if (
#if PG14_GE
				/* We only route UPDATE/DELETE through our CustomNode for PG 14+ because
				 * the codepath for earlier versions is different. */
				mt->operation == CMD_UPDATE || mt->operation == CMD_DELETE ||
#endif
				mt->operation == CMD_INSERT)
			{
				if (ht && (mt->operation == CMD_INSERT || !hypertable_is_distributed(ht)))
				{
					path = ts_hypertable_modify_path_create(root, mt, ht, input_rel);
				}
			}
#if PG15_GE
			if (ht && mt->operation == CMD_MERGE)
			{
				List *firstMergeActionList = linitial(mt->mergeActionLists);
				ListCell *l;
				/*
				 * Iterate over merge action to check if there is an INSERT sql.
				 * If so, then add ChunkDispatch node.
				 */
				foreach (l, firstMergeActionList)
				{
					MergeAction *action = (MergeAction *) lfirst(l);
					if (action->commandType == CMD_INSERT)
					{
						path = ts_hypertable_modify_path_create(root, mt, ht, input_rel);
						break;
					}
				}
			}
#endif
		}

		new_pathlist = lappend(new_pathlist, path);
	}

	return new_pathlist;
}

static void
timescaledb_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage,
									RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	Query *parse = root->parse;
	bool partials_found = false;
	TsRelType reltype = TS_REL_OTHER;
	Hypertable *ht = NULL;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (!ts_extension_is_loaded())
		return;

	if (input_rel != NULL)
		reltype = classify_relation(root, input_rel, &ht);

	if (ts_cm_functions->create_upper_paths_hook != NULL)
		ts_cm_functions
			->create_upper_paths_hook(root, stage, input_rel, output_rel, reltype, ht, extra);

	if (output_rel != NULL)
	{
		/* Modify for INSERTs on a hypertable */
		if (output_rel->pathlist != NIL)
			output_rel->pathlist =
				replace_hypertable_modify_paths(root, output_rel->pathlist, input_rel);

		if (parse->hasAggs && stage == UPPERREL_GROUP_AGG)
		{
			/* Existing AggPaths are modified here.
			 * No new AggPaths should be added after this if there
			 * are partials. */
			partials_found = ts_plan_process_partialize_agg(root, output_rel);
		}
	}

	if (!ts_guc_enable_optimizations || input_rel == NULL || IS_DUMMY_REL(input_rel))
		return;

	if (!involves_hypertable(root, input_rel))
		return;

	if (stage == UPPERREL_GROUP_AGG && output_rel != NULL)
	{
		if (!partials_found)
			ts_plan_add_hashagg(root, input_rel, output_rel);

		if (parse->hasAggs)
			ts_preprocess_first_last_aggregates(root, root->processed_tlist);
	}
}

static bool
contain_param_exec_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
		return true;

	return expression_tree_walker(node, contain_param_exec_walker, context);
}

bool
ts_contain_param(Node *node)
{
	return contain_param_exec_walker(node, NULL);
}

static List *
fill_missing_groupclause(List *new_groupclause, List *orig_groupclause)
{
	if (new_groupclause != NIL)
	{
		ListCell *gl;
		foreach (gl, orig_groupclause)
		{
			SortGroupClause *gc = lfirst_node(SortGroupClause, gl);

			if (list_member_ptr(new_groupclause, gc))
				continue; /* already in list */
			new_groupclause = lappend(new_groupclause, gc);
		}
	}
	return new_groupclause;
}

static bool
check_cagg_view_rte(RangeTblEntry *rte)
{
	ContinuousAgg *cagg = NULL;
	ListCell *rtlc;
	bool found = false;
	Query *viewq = rte->subquery;
	Assert(rte->rtekind == RTE_SUBQUERY);

	if (list_length(viewq->rtable) != 3) /* a view has 3 entries */
	{
		return false;
	}

	/* should cache this information for cont. aggregates */
	foreach (rtlc, viewq->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, rtlc);

		if (!OidIsValid(rte->relid))
			break;

		if ((cagg = ts_continuous_agg_find_by_relid(rte->relid)) != NULL)
			found = true;
	}
	return found;
}

/* Note that it modifies the passed in Query
* select * from (select a, b, max(c), min(d) from ...
				 group by a, b)
  order by b;
* is transformed as
* SELECT * from (select a, b, max(c), min(d) from ..
*                 group by B desc, A  <------ note the change in order here
*              )
*  order by b desc;
*  we transform only if order by is a subset of group-by
* transformation is applicable only to continuous aggregates
* Parameters:
* subq_rte - rte for subquery (inner query that will be modified)
* outer_sortcl -- outer query's sort clause
* outer_tlist - outer query's target list
*/
static void
cagg_reorder_groupby_clause(RangeTblEntry *subq_rte, Index rtno, List *outer_sortcl,
							List *outer_tlist)
{
	bool not_found = true;
	Query *subq;
	ListCell *lc;
	Assert(subq_rte->rtekind == RTE_SUBQUERY);
	subq = subq_rte->subquery;
	if (outer_sortcl && subq->groupClause && subq->sortClause == NIL &&
		check_cagg_view_rte(subq_rte))
	{
		List *new_groupclause = NIL;
		/* we are going to modify this. so make a copy and use it
		 if we replace */
		List *subq_groupclause_copy = copyObject(subq->groupClause);
		foreach (lc, outer_sortcl)
		{
			SortGroupClause *outer_sc = (SortGroupClause *) lfirst(lc);
			TargetEntry *outer_tle = get_sortgroupclause_tle(outer_sc, outer_tlist);
			not_found = true;
			if (IsA(outer_tle->expr, Var) && ((Index) ((Var *) outer_tle->expr)->varno == rtno))
			{
				int outer_attno = ((Var *) outer_tle->expr)->varattno;
				TargetEntry *subq_tle = list_nth(subq->targetList, outer_attno - 1);
				if (subq_tle->ressortgroupref > 0)
				{
					/* get group clause corresponding to this */
					SortGroupClause *subq_gclause =
						get_sortgroupref_clause(subq_tle->ressortgroupref, subq_groupclause_copy);
					subq_gclause->sortop = outer_sc->sortop;
					subq_gclause->nulls_first = outer_sc->nulls_first;
					Assert(subq_gclause->eqop == outer_sc->eqop);
					new_groupclause = lappend(new_groupclause, subq_gclause);
					not_found = false;
				}
			}
			if (not_found)
				break;
		}
		/* all order by found in group by clause */
		if (new_groupclause != NIL && not_found == false)
		{
			/* use new groupby clause for this subquery/view */
			subq->groupClause = fill_missing_groupclause(new_groupclause, subq_groupclause_copy);
		}
	}
}

/* Register our join pushdown hook. Becuase for PostgreSQL the tables we are operating
 * on are local tables. So, the FDW hooks are not called. Register our join path
 * generation as a generic planer hook.
 */

static void
timescaledb_set_join_pathlist_hook(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
								   RelOptInfo *innerrel, JoinType jointype,
								   JoinPathExtraData *extra)
{
	/* Left table has to be a distributed hypertable */
	TimescaleDBPrivate *outerrel_private = outerrel->fdw_private;
	if (outerrel_private != NULL && outerrel_private->fdw_relation_info != NULL)
		ts_cm_functions
			->mn_get_foreign_join_paths(root, joinrel, outerrel, innerrel, jointype, extra);

	/* Call next hook in chain */
	if (prev_set_join_pathlist_hook != NULL)
		(*prev_set_join_pathlist_hook)(root, joinrel, outerrel, innerrel, jointype, extra);
}

void
_planner_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = timescaledb_planner;
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_rel_pathlist_hook = timescaledb_set_rel_pathlist;
	set_join_pathlist_hook = timescaledb_set_join_pathlist_hook;

	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = timescaledb_get_relation_info_hook;

	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = timescaledb_create_upper_paths_hook;
}

void
_planner_fini(void)
{
	planner_hook = prev_planner_hook;
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	set_join_pathlist_hook = prev_set_join_pathlist_hook;
	get_relation_info_hook = prev_get_relation_info_hook;
	create_upper_paths_hook = prev_create_upper_paths_hook;
}

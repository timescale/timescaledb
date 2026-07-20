/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * A hypertable is normally expanded into one base relation per chunk at plan
 * time, which makes planning scale linearly with the chunk count. For a LIMIT
 * query that is unnecessary. This node lets the planner treat the hypertable as
 * a single relation and defers chunk enumeration and scanning to execution time,
 * where the LIMIT stops the scan early.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/stratnum.h>
#include <access/sysattr.h>
#include <catalog/pg_type.h>
#include <commands/explain.h>
#include <commands/explain_format.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <parser/parsetree.h>
#include <storage/lockdefs.h>
#include <tcop/dest.h>
#include <tcop/pquery.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/portal.h>
#include <utils/rel.h>
#include <utils/ruleutils.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "chunk.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "guc.h"
#include "hypertable.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/hypertable_scan/hypertable_scan.h"
#include "scan_iterator.h"
#include "ts_catalog/catalog.h"
#include "utils.h"

/* Rows fetched per PortalRunFetch when there is no pushed LIMIT to bound it. */
#define HS_FETCH_BATCH_SIZE 1000

/*
 * custom_private is a two-element list: an int list (indexed by the enum below)
 * and a single-element oid list holding the sort operator for EXPLAIN.
 */
typedef enum
{
	HS_PRIV_ORDERED = 0, /* 1 if ordered by the primary dimension */
	HS_PRIV_LIMIT,		 /* rows to push into each per-chunk query, 0 = none */
	HS_PRIV_SORT_ATTNO,	 /* primary-dimension column (ordered mode) */
	HS_PRIV_DESCENDING,	 /* 1 if the ordered sort is descending */
	HS_PRIV_NULLS_FIRST, /* 1 if the ordered sort is NULLS FIRST */
	HS_PRIV_COUNT
} HypertableScanPrivIndex;

typedef struct HypertableScanState
{
	CustomScanState css;

	bool ordered;
	AttrNumber sort_attno;
	bool descending;
	Oid sortop;		  /* sort operator, for EXPLAIN */
	bool nulls_first; /* NULLS FIRST, for EXPLAIN */
	int push_limit;

	int num_fetch_attnos;
	AttrNumber *fetch_attnos;

	int32 hypertable_id;
	int32 primary_dimension_id;
	int64 last_range_start;			/* ordered: range_start of the last slice returned */
	TimestampTz last_creation_time; /* unordered: creation_time of the last chunk returned */
	int32 last_id;					/* unordered: id of the last chunk (creation_time tiebreak) */
	bool have_last;					/* whether the last_* resume key is set */
	int chunks_scanned;				/* chunks opened so far (EXPLAIN counter) */

	/*
	 * Each chunk is scanned through its own Portal, fetching a batch of rows at a
	 * time into chunk_mcxt so a large chunk is not fully materialized.
	 */
	int batch_size;			  /* rows per PortalRunFetch */
	Portal cur_portal;		  /* open portal for the current chunk, or NULL */
	DestReceiver *dest;		  /* receiver that copies fetched rows into chunk_mcxt */
	MemoryContext chunk_mcxt; /* holds the current batch's tuples */
	HeapTuple *cur_tuples;	  /* current batch (batch_size long), in chunk_mcxt */
	TupleDesc cur_tupdesc;	  /* per-chunk query row type (node-lifetime copy) */
	uint64 cur_nrows;
	uint64 cur_row;
	TupleTableSlot *scan_slot; /* virtual tuple in hypertable row type, before projection */
} HypertableScanState;

/* DestReceiver that copies each fetched row into the scan's chunk_mcxt. */
typedef struct HypertableScanDest
{
	DestReceiver pub;
	HypertableScanState *state;
} HypertableScanDest;

static bool
hs_receive_slot(TupleTableSlot *slot, DestReceiver *self)
{
	HypertableScanState *state = ((HypertableScanDest *) self)->state;
	MemoryContext old = MemoryContextSwitchTo(state->chunk_mcxt);
	state->cur_tuples[state->cur_nrows++] = ExecCopySlotHeapTuple(slot);
	MemoryContextSwitchTo(old);
	return true;
}

static void
hs_dest_noop(DestReceiver *self)
{
}
static void
hs_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
}

static DestReceiver *
hs_create_dest(HypertableScanState *state)
{
	HypertableScanDest *dest = palloc0(sizeof(HypertableScanDest));
	dest->pub.receiveSlot = hs_receive_slot;
	dest->pub.rStartup = hs_dest_startup;
	dest->pub.rShutdown = hs_dest_noop;
	dest->pub.rDestroy = hs_dest_noop;
	dest->pub.mydest = DestNone;
	dest->state = state;
	return (DestReceiver *) dest;
}

static Plan *hypertable_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
										 List *tlist, List *clauses, List *custom_plans);
static Node *hypertable_scan_state_create(CustomScan *cscan);
static void hypertable_scan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *hypertable_scan_exec(CustomScanState *node);
static void hypertable_scan_end(CustomScanState *node);
static void hypertable_scan_rescan(CustomScanState *node);
static void hypertable_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomPathMethods hypertable_scan_path_methods = {
	.CustomName = "HypertableScan",
	.PlanCustomPath = hypertable_scan_plan_create,
};

static CustomScanMethods hypertable_scan_plan_methods = {
	.CustomName = "HypertableScan",
	.CreateCustomScanState = hypertable_scan_state_create,
};

static CustomExecMethods hypertable_scan_exec_methods = {
	.CustomName = "HypertableScan",
	.BeginCustomScan = hypertable_scan_begin,
	.ExecCustomScan = hypertable_scan_exec,
	.EndCustomScan = hypertable_scan_end,
	.ReScanCustomScan = hypertable_scan_rescan,
	.ExplainCustomScan = hypertable_scan_explain,
};

/* The single ORDER BY key, if any, that we can satisfy for this hypertable. */
static SortGroupClause *
primary_dimension_sort_key(const Query *query, const Hypertable *ht)
{
	if (query->sortClause == NIL)
	{
		return NULL;
	}

	if (list_length(query->sortClause) != 1)
	{
		return NULL;
	}

	const Dimension *dim = hyperspace_get_open_dimension(ht->space, 0);
	if (dim == NULL)
	{
		return NULL;
	}

	/* Only support single-column primary dimensions for now. */
	if (ht->space->num_dimensions != 1)
	{
		return NULL;
	}

	SortGroupClause *sgc = linitial_node(SortGroupClause, query->sortClause);
	TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);
	if (!tle || !IsA(tle->expr, Var))
	{
		return NULL;
	}

	Var *var = (Var *) tle->expr;
	if (var->varlevelsup != 0 || var->varattno != dim->column_attno)
	{
		return NULL;
	}

	return sgc;
}

static bool
hypertable_scan_is_candidate(const Query *query, const Hypertable *ht)
{
	if (!ts_guc_enable_hypertablescan || ht == NULL)
	{
		return false;
	}

	if (query->commandType != CMD_SELECT || !query->limitCount ||
		query->limitOption != LIMIT_OPTION_COUNT)
	{
		return false;
	}

	if (query->hasAggs || query->groupClause || query->groupingSets || query->hasWindowFuncs ||
		query->distinctClause || query->setOperations || query->havingQual ||
		query->hasModifyingCTE || query->rowMarks)
	{
		return false;
	}

	/*
	 * Exactly one from-list entry, and no WHERE for now.
	 */
	if (query->jointree == NULL || list_length(query->jointree->fromlist) != 1 ||
		query->jointree->quals != NULL)
	{
		return false;
	}

	/*
	 * The single entry must be the hypertable itself as a plain relation reference.
	 */
	Node *fl = linitial(query->jointree->fromlist);
	if (!IsA(fl, RangeTblRef))
	{
		return false;
	}

	Index rtindex = ((RangeTblRef *) fl)->rtindex;
	RangeTblEntry *rte = rt_fetch(rtindex, query->rtable);
	if (rte->rtekind != RTE_RELATION || rte->relid != ht->main_table_relid ||
		rte->tablesample != NULL)
	{
		return false;
	}

	/*
	 * RLS policies attach to the hypertable, not its chunks. Scanning chunks
	 * directly would bypass them, so fall back to the expanded plan when the
	 * hypertable has row security or policies already applied to this query.
	 */
	if (rte->securityQuals != NIL || ts_has_row_security(ht->main_table_relid))
	{
		return false;
	}

	/*
	 * Reject system-column references (ctid, tableoid, ...) for now
	 */
	Bitmapset *attrs = NULL;
	pull_varattnos((Node *) query->targetList, rtindex, &attrs);
	int x = -1;
	while ((x = bms_next_member(attrs, x)) >= 0)
	{
		if (x + FirstLowInvalidHeapAttributeNumber < 0)
		{
			return false;
		}
	}

	if (query->sortClause != NIL && primary_dimension_sort_key(query, ht) == NULL)
	{
		return false;
	}

	return true;
}

/*
 * Whether the query can be served by the HypertableScan node. In debug builds
 * this also enforces debug_require_hypertable_scan so tests can assert the node
 * choice without depending on version-specific EXPLAIN output; the sole caller
 * invokes this only at the hypertable's own planning decision.
 */
bool
ts_should_hypertable_scan(const Query *query, const Hypertable *ht)
{
	bool used = hypertable_scan_is_candidate(query, ht);

#ifdef TS_DEBUG
	if (!used && ts_guc_debug_require_hypertable_scan == DRO_Require)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("HypertableScan not used when required by the "
						"debug_require_hypertable_scan GUC")));
	}
	if (used && ts_guc_debug_require_hypertable_scan == DRO_Forbid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("HypertableScan used when forbidden by the "
						"debug_require_hypertable_scan GUC")));
	}
#endif

	return used;
}

/*
 * When the LIMIT (and OFFSET) are plain constants, offset+count can be pushed
 * into each per-chunk query.
 */
static int64
hypertable_scan_push_limit(const Query *query)
{
	Node *lc = query->limitCount;
	Node *lo = query->limitOffset;

	if (lc == NULL || !IsA(lc, Const) || ((Const *) lc)->consttype != INT8OID ||
		((Const *) lc)->constisnull)
	{
		return 0;
	}
	int64 count = DatumGetInt64(((Const *) lc)->constvalue);
	if (count < 0)
	{
		return 0;
	}

	int64 offset = 0;
	if (lo != NULL)
	{
		if (!IsA(lo, Const) || ((Const *) lo)->consttype != INT8OID || ((Const *) lo)->constisnull)
		{
			return 0; /* non-constant offset: don't push down */
		}
		offset = Max(DatumGetInt64(((Const *) lo)->constvalue), 0);
	}

	/* Don't push down if offset+count overflows, or exceeds the int push_limit. */
	int64 sum;
	if (pg_add_s64_overflow(count, offset, &sum) || sum > INT_MAX)
	{
		return 0;
	}
	return sum;
}

void
ts_hypertable_scan_add_path(PlannerInfo *root, RelOptInfo *rel, const Hypertable *ht)
{
	CustomPath *cpath = makeNode(CustomPath);
	AttrNumber sort_attno = 0;
	bool descending = false;
	Oid sortop = InvalidOid;
	bool nulls_first = false;
	int64 push_limit = hypertable_scan_push_limit(root->parse);

	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.rows = clamp_row_est(rel->rows);
	/* startup_cost = locate+open the first chunk */
	cpath->path.startup_cost = random_page_cost;
	/* total_cost grows with rows so an enclosing Limit scales it */
	cpath->path.total_cost = cpath->path.startup_cost + cpath->path.rows * 2 * cpu_tuple_cost;
	cpath->methods = &hypertable_scan_path_methods;

	SortGroupClause *sgc = primary_dimension_sort_key(root->parse, ht);
	if (sgc != NULL)
	{
		TargetEntry *tle = get_sortgroupclause_tle(sgc, root->parse->targetList);
		Var *var = (Var *) tle->expr;
		Oid opfamily;
		Oid opcintype;
		CompareType cmptype;

		sort_attno = var->varattno;
		if (!get_ordering_op_properties(sgc->sortop, &opfamily, &opcintype, &cmptype))
		{
			elog(ERROR, "sort operator %u is not an ordering operator", sgc->sortop);
		}
		descending = (cmptype == COMPARE_GT);
		sortop = sgc->sortop;
		nulls_first = sgc->nulls_first;
		/* Advertise the ORDER BY ordering we produce so no Sort is added. */
		cpath->path.pathkeys = root->sort_pathkeys;
	}
	else
	{
		cpath->path.pathkeys = NIL;
	}

	List *ints = list_make4_int(sgc != NULL, (int) push_limit, sort_attno, descending);
	ints = lappend_int(ints, nulls_first);
	cpath->custom_private = list_make2(ints, list_make1_oid(sortop));

	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;
	add_path(rel, &cpath->path);
	set_cheapest(rel);
}

static Plan *
hypertable_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
							List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->custom_plans = custom_plans;
	cscan->custom_private = best_path->custom_private;
	cscan->custom_scan_tlist = NIL;
	cscan->methods = &hypertable_scan_plan_methods;

	return &cscan->scan.plan;
}

static Node *
hypertable_scan_state_create(CustomScan *cscan)
{
	HypertableScanState *state =
		(HypertableScanState *) newNode(sizeof(HypertableScanState), T_CustomScanState);

	state->css.methods = &hypertable_scan_exec_methods;

	List *ints = linitial(cscan->custom_private);
	Assert(list_length(ints) == HS_PRIV_COUNT);
	state->ordered = list_nth_int(ints, HS_PRIV_ORDERED) != 0;
	state->push_limit = list_nth_int(ints, HS_PRIV_LIMIT);
	state->sort_attno = (AttrNumber) list_nth_int(ints, HS_PRIV_SORT_ATTNO);
	state->descending = list_nth_int(ints, HS_PRIV_DESCENDING) != 0;
	state->nulls_first = list_nth_int(ints, HS_PRIV_NULLS_FIRST) != 0;
	state->sortop = linitial_oid(lsecond(cscan->custom_private));

	return (Node *) state;
}

/*
 * Determine which columns to fetch from this scan's output targetlist
 */
static void
compute_fetch_columns(HypertableScanState *state, CustomScanState *node)
{
	Scan *scan = (Scan *) node->ss.ps.plan;
	TupleDesc desc = RelationGetDescr(node->ss.ss_currentRelation);
	Bitmapset *attrs = NULL;
	pull_varattnos((Node *) scan->plan.targetlist, scan->scanrelid, &attrs);

	/*
	 * A whole-row reference (attno 0) needs every column; expand it to all live
	 * columns so it goes through the same by-attno assignment as any other column
	 * set. (System columns, attno < 0, are rejected in ts_should_hypertable_scan.)
	 */
	bool whole_row = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs);

	state->fetch_attnos = palloc(sizeof(AttrNumber) * desc->natts);
	state->num_fetch_attnos = 0;
	if (whole_row)
	{
		for (int a = 0; a < desc->natts; a++)
		{
			if (!TupleDescAttr(desc, a)->attisdropped)
			{
				state->fetch_attnos[state->num_fetch_attnos++] = a + 1;
			}
		}
	}
	else
	{
		int i = -1;
		while ((i = bms_next_member(attrs, i)) >= 0)
		{
			state->fetch_attnos[state->num_fetch_attnos++] =
				(AttrNumber) (i + FirstLowInvalidHeapAttributeNumber);
		}
	}
}

/* Reset the scan position to the start */
static void
hypertable_scan_reset(HypertableScanState *state)
{
	if (state->cur_portal != NULL)
	{
		PortalDrop(state->cur_portal, false);
		state->cur_portal = NULL;
	}
	state->cur_nrows = 0;
	state->cur_row = 0;
	state->have_last = false;
	state->chunks_scanned = 0;
	if (state->chunk_mcxt != NULL)
	{
		MemoryContextReset(state->chunk_mcxt);
	}
}

static void
hypertable_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableScanState *state = (HypertableScanState *) node;
	Oid ht_relid = RelationGetRelid(node->ss.ss_currentRelation);

	compute_fetch_columns(state, node);
	state->chunk_mcxt =
		AllocSetContextCreate(estate->es_query_cxt, "HypertableScan chunk", ALLOCSET_DEFAULT_SIZES);
	state->dest = hs_create_dest(state);
	hypertable_scan_reset(state);

	state->scan_slot =
		MakeSingleTupleTableSlot(RelationGetDescr(node->ss.ss_currentRelation), &TTSOpsVirtual);

	/*
	 * Non-fetched columns are always NULL, and their isnull flags never change
	 * between rows, so set them once here instead of per row. Fetched positions
	 * are overwritten for every row.
	 */
	memset(state->scan_slot->tts_isnull,
		   true,
		   state->scan_slot->tts_tupleDescriptor->natts * sizeof(bool));

	state->hypertable_id = ts_hypertable_relid_to_id(ht_relid);
	state->primary_dimension_id = 0;
	if (state->ordered)
	{
		Hypertable *ht = ts_hypertable_get_by_id(state->hypertable_id);
		state->primary_dimension_id = hyperspace_get_open_dimension(ht->space, 0)->fd.id;
	}

	/*
	 * Cap the batch at HS_FETCH_BATCH_SIZE: the per-chunk query already carries
	 * the LIMIT, and the fetch loop refills across batches, so a large pushed
	 * LIMIT must not turn into one oversized cur_tuples allocation.
	 */
	state->batch_size =
		state->push_limit > 0 ? Min(state->push_limit, HS_FETCH_BATCH_SIZE) : HS_FETCH_BATCH_SIZE;
}

/*
 * Build the per-chunk query.
 */
static char *
hypertable_scan_chunk_sql(HypertableScanState *state, Oid reloid)
{
	TupleDesc desc = RelationGetDescr(state->css.ss.ss_currentRelation);
	char *qualified = quote_qualified_identifier(get_namespace_name(get_rel_namespace(reloid)),
												 get_rel_name(reloid));
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfoString(&sql, "SELECT ");
	for (int i = 0; i < state->num_fetch_attnos; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, state->fetch_attnos[i] - 1);
		appendStringInfo(&sql, "%s%s", i > 0 ? ", " : "", quote_identifier(NameStr(att->attname)));
	}
	appendStringInfo(&sql, " FROM %s", qualified);
	if (state->ordered)
	{
		Form_pg_attribute att = TupleDescAttr(desc, state->sort_attno - 1);
		appendStringInfo(&sql,
						 " ORDER BY %s %s",
						 quote_identifier(NameStr(att->attname)),
						 state->descending ? "DESC" : "ASC");
	}
	if (state->push_limit > 0)
	{
		appendStringInfo(&sql, " LIMIT %d", state->push_limit);
	}
	return sql.data;
}

/*
 * In ordered mode we scan dimension slices ordered by range_start.
 */
static Oid
next_chunk_reloid_ordered(HypertableScanState *state)
{
	StrategyNumber start_strategy = InvalidStrategy;
	int64 start_value = 0;
	if (state->have_last)
	{
		start_strategy = state->descending ? BTLessStrategyNumber : BTGreaterStrategyNumber;
		start_value = state->last_range_start;
	}

	ScanIterator it = ts_dimension_slice_scan_iterator_create(NULL, CurrentMemoryContext);
	ts_dimension_slice_scan_iterator_set_range(&it,
											   state->primary_dimension_id,
											   start_strategy,
											   start_value,
											   InvalidStrategy,
											   0);
	it.ctx.scandirection = state->descending ? BackwardScanDirection : ForwardScanDirection;
	ts_scan_iterator_start_scan(&it);

	TupleInfo *ti = ts_scan_iterator_next(&it);
	if (ti == NULL)
	{
		ts_scan_iterator_close(&it);
		return InvalidOid;
	}

	bool isnull;
	int32 chunk_id = DatumGetInt32(slot_getattr(ti->slot, Anum_dimension_slice_chunk_id, &isnull));
	state->last_range_start =
		DatumGetInt64(slot_getattr(ti->slot, Anum_dimension_slice_range_start, &isnull));
	state->have_last = true;
	ts_scan_iterator_close(&it);

	return ts_chunk_get_relid(chunk_id, /* missing_ok = */ false);
}

/*
 * In unordered mode we scan chunks in creation_time DESC order, with id DESC as a
 * tie breaker. In theory creation_time could have duplicates so we need to account
 * for that.
 */
static Oid
next_chunk_reloid_unordered(HypertableScanState *state)
{
	ScanIterator it = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	it.ctx.index =
		catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_HYPERTABLE_ID_CREATION_TIME_INDEX);
	ts_scan_iterator_scan_key_init(&it,
								   Anum_chunk_hypertable_id_creation_time_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(state->hypertable_id));
	if (state->have_last)
	{
		ts_scan_iterator_scan_key_init(&it,
									   Anum_chunk_hypertable_id_creation_time_idx_creation_time,
									   BTLessEqualStrategyNumber,
									   F_TIMESTAMPTZ_LE,
									   TimestampTzGetDatum(state->last_creation_time));
	}
	it.ctx.scandirection = BackwardScanDirection;
	ts_scan_iterator_start_scan(&it);

	Oid best_reloid = InvalidOid;
	int32 best_id = 0;
	TimestampTz best_ct = 0;
	bool found = false;
	TupleInfo *ti;
	while ((ti = ts_scan_iterator_next(&it)) != NULL)
	{
		bool isnull;
		Oid r = DatumGetObjectId(slot_getattr(ti->slot, Anum_chunk_relid, &isnull));
		if (isnull || !OidIsValid(r))
		{
			continue;
		}
		int32 id = DatumGetInt32(slot_getattr(ti->slot, Anum_chunk_id, &isnull));
		TimestampTz ct =
			DatumGetTimestampTz(slot_getattr(ti->slot, Anum_chunk_creation_time, &isnull));

		/* Already returned: same creation_time and id >= last_id (ids descend within
		 * a tie, so everything from last_id up was already emitted). */
		if (state->have_last && ct == state->last_creation_time && id >= state->last_id)
		{
			continue;
		}
		/* Scan is creation_time-descending; once past the best group, later groups
		 * have a smaller creation_time and can't win. */
		if (found && ct < best_ct)
		{
			break;
		}
		/* Highest creation_time wins; highest id within it. */
		if (!found || id > best_id)
		{
			best_reloid = r;
			best_id = id;
			best_ct = ct;
			found = true;
		}
	}
	ts_scan_iterator_close(&it);

	if (found)
	{
		state->last_creation_time = best_ct;
		state->last_id = best_id;
		state->have_last = true;
	}
	return best_reloid;
}

/* Next chunk relation to scan, or InvalidOid when exhausted. */
static Oid
next_chunk_reloid(HypertableScanState *state)
{
	return state->ordered ? next_chunk_reloid_ordered(state) : next_chunk_reloid_unordered(state);
}

/*
 * Open a Portal for the next chunk's per-chunk query. Returns false when there
 * are no more chunks.
 */
static bool
open_next_chunk(HypertableScanState *state)
{
	Oid reloid = next_chunk_reloid(state);
	if (!OidIsValid(reloid))
	{
		return false;
	}
	state->chunks_scanned++;

	char *sql = hypertable_scan_chunk_sql(state, reloid);
	List *parsetree = pg_parse_query(sql);
	RawStmt *raw = linitial_node(RawStmt, parsetree);
	List *querytree = pg_analyze_and_rewrite_fixedparams(raw, sql, NULL, 0, NULL);
	Query *query = linitial_node(Query, querytree);
	PlannedStmt *plan =
		pg_plan_query(query, sql, CURSOR_OPT_FAST_PLAN | CURSOR_OPT_NO_SCROLL, NULL);

	Portal portal = CreateNewPortal();
	portal->visible = false;
	PortalDefineQuery(portal, NULL, sql, CMDTAG_SELECT, list_make1(plan), NULL);
	PortalStart(portal, NULL, 0, GetActiveSnapshot());
	state->cur_portal = portal;

	/* Per-chunk result row type is the same for every chunk; capture it once. */
	if (state->cur_tupdesc == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(state->css.ss.ps.state->es_query_cxt);
		state->cur_tupdesc = CreateTupleDescCopy(portal->tupDesc);
		MemoryContextSwitchTo(old);
	}

	pfree(sql);
	return true;
}

/* Make the next row of the current chunk available in fetch_slot, refilling the
 * batch and advancing to the next chunk as needed. False when fully exhausted. */
static bool
next_chunk_row(HypertableScanState *state)
{
	for (;;)
	{
		if (state->cur_row < state->cur_nrows)
		{
			return true; /* buffered row available */
		}
		if (state->cur_portal == NULL && !open_next_chunk(state))
		{
			return false; /* no more chunks */
		}
		/* Fetch the next batch into cur_tuples via our receiver. */
		MemoryContextReset(state->chunk_mcxt);
		state->cur_tuples =
			MemoryContextAlloc(state->chunk_mcxt, sizeof(HeapTuple) * state->batch_size);
		state->cur_nrows = 0;
		state->cur_row = 0;
		PortalRunFetch(state->cur_portal, FETCH_FORWARD, state->batch_size, state->dest);
		if (state->cur_nrows == 0)
		{
			/* Chunk exhausted: drop the portal and move to the next chunk. */
			PortalDrop(state->cur_portal, false);
			state->cur_portal = NULL;
		}
	}
}

static TupleTableSlot *
hypertable_scan_next(ScanState *ss)
{
	HypertableScanState *state = (HypertableScanState *) ss;

	if (!next_chunk_row(state))
	{
		return NULL;
	}

	HeapTuple tuple = state->cur_tuples[state->cur_row++];

	/*
	 * Assign the fetched columns to their hypertable attno; the rest stay NULL
	 * (isnull was set once in begin). Fetched column i+1 is fetch_attnos[i].
	 */
	TupleTableSlot *slot = state->scan_slot;
	ExecClearTuple(slot);
	for (int i = 0; i < state->num_fetch_attnos; i++)
	{
		bool isnull;
		Datum v = heap_getattr(tuple, i + 1, state->cur_tupdesc, &isnull);
		AttrNumber attno = state->fetch_attnos[i];
		slot->tts_values[attno - 1] = v;
		slot->tts_isnull[attno - 1] = isnull;
	}
	ExecStoreVirtualTuple(slot);

	/* Copy into the scan slot ExecScan projects from; this materializes the byref
	 * data so it survives resetting chunk_mcxt for the next batch. */
	ExecCopySlot(ss->ss_ScanTupleSlot, slot);
	return ss->ss_ScanTupleSlot;
}

static bool
hypertable_scan_recheck(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
hypertable_scan_exec(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) hypertable_scan_next,
					(ExecScanRecheckMtd) hypertable_scan_recheck);
}

static void
hypertable_scan_end(CustomScanState *node)
{
	HypertableScanState *state = (HypertableScanState *) node;

	if (state->cur_portal != NULL)
	{
		PortalDrop(state->cur_portal, false);
		state->cur_portal = NULL;
	}
	if (state->scan_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->scan_slot);
		state->scan_slot = NULL;
	}
	if (state->chunk_mcxt != NULL)
	{
		MemoryContextDelete(state->chunk_mcxt);
		state->chunk_mcxt = NULL;
	}
}

static void
hypertable_scan_rescan(CustomScanState *node)
{
	hypertable_scan_reset((HypertableScanState *) node);
}

/* Show the ORDER BY key (ordered mode) and, under ANALYZE, how many chunks the
 * LIMIT visited. */
static void
hypertable_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	HypertableScanState *state = (HypertableScanState *) node;

	if (state->ordered)
	{
		Plan *plan = node->ss.ps.plan;
		TupleDesc desc = RelationGetDescr(node->ss.ss_currentRelation);
		Form_pg_attribute att = TupleDescAttr(desc, state->sort_attno - 1);
		Var *var = makeVar(((Scan *) plan)->scanrelid,
						   state->sort_attno,
						   att->atttypid,
						   att->atttypmod,
						   att->attcollation,
						   0);
		List *context = set_deparse_context_plan(es->deparse_cxt, plan, ancestors);
		bool useprefix = list_length(es->rtable) > 1 || es->verbose;
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfoString(&buf, deparse_expression((Node *) var, context, useprefix, true));
		ts_show_sortorder_options(&buf,
								  (Node *) var,
								  state->sortop,
								  att->attcollation,
								  state->nulls_first);
		ExplainPropertyList("Order", list_make1(buf.data), es);
	}
	/* Under ANALYZE, how many chunks the LIMIT actually made us visit. */
	if (es->analyze)
	{
		ExplainPropertyInteger("Chunks Visited", NULL, state->chunks_scanned, es);
	}
}

void
_hypertable_scan_init(void)
{
	TryRegisterCustomScanMethods(&hypertable_scan_plan_methods);
}

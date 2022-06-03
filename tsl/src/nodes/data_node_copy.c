/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/transam.h>
#include <executor/executor.h>
#include <executor/nodeModifyTable.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
#include <funcapi.h>
#include <miscadmin.h>

#include <hypertable_cache.h>
#include <nodes/chunk_dispatch_state.h>
#include <nodes/chunk_insert_state.h>
#include <nodes/chunk_dispatch_plan.h>
#include <compat/compat.h>
#include <guc.h>

#include "data_node_copy.h"
#include "remote/dist_copy.h"

typedef struct DataNodeCopyPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Index hypertable_rti; /* range table index of Hypertable */
	int subplan_index;
} DataNodeCopyPath;

/*
 * DataNodeCopy dispatches tuples to data nodes using batching. It inserts
 * itself below a ModifyTable node in the plan and subsequent execution tree,
 * like so:
 *
 *          --------------------   Set "direct modify plans" to
 *          | HypertableInsert |   signal ModifyTable to only
 *          --------------------   handle returning projection.
 *                   |
 *            ----------------     resultRelInfo->ri_usesFdwDirectModify
 *            |  ModifyTable |     should be TRUE. Handle returning projection.
 *            ----------------
 *                   ^
 *                   | RETURNING tuple or nothing
 *            ----------------
 *            | DataNodeCopy |     Send tuple to data nodes in COPY_IN state.
 *            ----------------
 *                   ^
 *                   | Chunk-routed tuple
 *           -----------------
 *           | ChunkDispatch |     Route tuple to chunk.
 *           -----------------     Set es_result_relation.
 *                   ^
 *                   | tuple
 *             --------------
 *             | ValuesScan |     VALUES ('2019-02-23 13:43', 1, 8.9),
 *             --------------            ('2019-02-23 13:46', 2, 1.5);
 *
 */
typedef struct DataNodeCopyState
{
	CustomScanState cstate;
	Relation rel;		/* The (local) relation we're inserting into */
	bool set_processed; /* Indicates whether to set the number or processed tuples */
	Cache *hcache;
	Hypertable *ht;
	RemoteCopyContext *copy_ctx;
} DataNodeCopyState;

/*
 * Plan metadata list indexes.
 */
enum CustomScanPrivateIndex
{
	CustomScanPrivateTargetAttrs,
	CustomScanPrivateSetProcessed,
	CustomScanPrivateBinaryPossible,
};

static List *
generate_attname_list(const TupleDesc tupdesc, const List *target_attrs)
{
	List *attlist = NIL;
	ListCell *lc;

	foreach (lc, target_attrs)
	{
		AttrNumber attnum = lfirst_int(lc);
		Form_pg_attribute attr = &tupdesc->attrs[AttrNumberGetAttrOffset(attnum)];

		Assert(!attr->attisdropped);
		attlist = lappend(attlist, makeString(NameStr(attr->attname)));
	}

	return attlist;
}

static void
data_node_copy_begin(CustomScanState *node, EState *estate, int eflags)
{
	DataNodeCopyState *dncs = (DataNodeCopyState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
#if PG14_LT
	ResultRelInfo *rri = estate->es_result_relation_info;
#else
	ResultRelInfo *rri = linitial_node(ResultRelInfo, estate->es_opened_result_relations);
#endif
	Relation rel = rri->ri_RelationDesc;
	Plan *subplan = linitial(cscan->custom_plans);
	List *target_attrs = list_nth(cscan->custom_private, CustomScanPrivateTargetAttrs);
	bool set_processed = intVal(list_nth(cscan->custom_private, CustomScanPrivateSetProcessed));
	bool binary_possible = intVal(list_nth(cscan->custom_private, CustomScanPrivateBinaryPossible));
	bool use_binary_encoding = ts_guc_enable_connection_binary_data;
	PlanState *ps;
	CopyStmt copy_stmt = {
		.type = T_CopyStmt,
		.is_from = true,
		.relation = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								 RelationGetRelationName(rel),
								 0),
		.attlist = generate_attname_list(RelationGetDescr(rel), target_attrs),
		.options = NIL,
	};

	dncs->ht = ts_hypertable_cache_get_cache_and_entry(rel->rd_id, CACHE_FLAG_NONE, &dncs->hcache);
	Assert(hypertable_is_distributed(dncs->ht));

	if (!binary_possible)
		use_binary_encoding = false;

	ps = ExecInitNode(subplan, estate, eflags);

	node->custom_ps = list_make1(ps);
	dncs->rel = rel;
	dncs->set_processed = set_processed;
	dncs->copy_ctx = remote_copy_begin(&copy_stmt,
									   dncs->ht,
									   GetPerTupleExprContext(estate),
									   target_attrs,
									   use_binary_encoding);
}

/*
 * Execute the remote INSERT.
 *
 * This is called every time the parent asks for a new tuple. Read the child
 * scan node and buffer until there's a full batch, then flush by sending to
 * data node(s). If there's a returning statement, we return the flushed tuples
 * one-by-one, or continue reading more tuples from the child until there's a
 * NULL tuple.
 */
static TupleTableSlot *
data_node_copy_exec(CustomScanState *node)
{
	DataNodeCopyState *dncs = (DataNodeCopyState *) node;
	PlanState *substate = linitial(dncs->cstate.custom_ps);
	ChunkDispatchState *cds = (ChunkDispatchState *) substate;
	EState *estate = node->ss.ps.state;
#if PG14_LT
	ResultRelInfo *rri_saved = estate->es_result_relation_info;
#else
	ResultRelInfo *rri_saved = linitial_node(ResultRelInfo, estate->es_opened_result_relations);
#endif
	TupleTableSlot *slot;
	bool has_returning = rri_saved->ri_projectReturning != NULL;

#if PG14_LT
	/* Initially, the result relation should always match the hypertable.  */
	Assert(node->ss.ps.state->es_result_relation_info->ri_RelationDesc->rd_id == dncs->rel->rd_id);
#endif

	do
	{
		slot = ExecProcNode(substate);

		if (!TupIsNull(slot))
		{
			/* The child node (ChunkDispatch) routed the tuple to the right
			 * chunk result relation */
			ResultRelInfo *rri_chunk = cds->rri;
			const ChunkInsertState *cis = rri_chunk->ri_FdwState;
			MemoryContext oldmctx;
			bool success;
			const TupleDesc rri_desc = RelationGetDescr(rri_chunk->ri_RelationDesc);

			if (NULL != rri_chunk->ri_projectReturning && rri_desc->constr &&
				rri_desc->constr->has_generated_stored)
				ExecComputeStoredGeneratedCompat(rri_chunk, estate, slot, CMD_INSERT);

			ResetPerTupleExprContext(estate);
			oldmctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
			success = remote_copy_send_slot(dncs->copy_ctx, slot, cis);
			MemoryContextSwitchTo(oldmctx);

			if (!success)
				slot = ExecClearTuple(slot);
			else
			{
				if (has_returning)
				{
					ExprContext *econtext;

					Assert(NULL != rri_saved->ri_projectReturning);
					econtext = rri_saved->ri_projectReturning->pi_exprContext;
					econtext->ecxt_scantuple = slot;
				}

				if (dncs->set_processed)
					estate->es_processed++;
			}
		}
	} while (!has_returning && !TupIsNull(slot));

#if PG14_LT
	/* Reset the result relation to point to the root hypertable before
	 * returning, since the child ChunkDispatch node set it to the chunk. */
	estate->es_result_relation_info = rri_saved;

	/* Tuple routing in the ChunkDispatchState subnode sets the result
	 * relation to a chunk when routing, but the read handler should have
	 * ensured the result relation is reset. */
	Assert(node->ss.ps.state->es_result_relation_info->ri_RelationDesc->rd_id == dncs->rel->rd_id);
	Assert(node->ss.ps.state->es_result_relation_info->ri_usesFdwDirectModify);
#endif

	return slot;
}

static void
data_node_copy_rescan(CustomScanState *node)
{
	/* Cannot rescan and start from the beginning since we might already have
	 * sent data to remote nodes */
	elog(ERROR, "cannot restart inserts to remote nodes");
}

static void
data_node_copy_end(CustomScanState *node)
{
	DataNodeCopyState *dncs = (DataNodeCopyState *) node;

	ExecEndNode(linitial(node->custom_ps));
	remote_copy_end(dncs->copy_ctx);
	ts_cache_release(dncs->hcache);
}

static void
data_node_copy_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DataNodeCopyState *dncs = (DataNodeCopyState *) node;
	/*
	 * Add remote COPY command when VERBOSE option is specified.
	 */
	if (es->verbose)
		ExplainPropertyText("Remote SQL", remote_copy_get_copycmd(dncs->copy_ctx), es);
}

static CustomExecMethods data_node_copy_state_methods = {
	.CustomName = "DataNodeCopyState",
	.BeginCustomScan = data_node_copy_begin,
	.EndCustomScan = data_node_copy_end,
	.ExecCustomScan = data_node_copy_exec,
	.ReScanCustomScan = data_node_copy_rescan,
	.ExplainCustomScan = data_node_copy_explain,
};

/* Only allocate the custom scan state. Initialize in the begin handler. */
static Node *
data_node_copy_state_create(CustomScan *cscan)
{
	DataNodeCopyState *dncs;

	dncs = (DataNodeCopyState *) newNode(sizeof(DataNodeCopyState), T_CustomScanState);
	dncs->cstate.methods = &data_node_copy_state_methods;

	return (Node *) dncs;
}

static CustomScanMethods data_node_copy_plan_methods = {
	.CustomName = "DataNodeCopy",
	.CreateCustomScanState = data_node_copy_state_create,
};

/*
 * Check for a user-created (custom) type.
 *
 * PostgreSQL reserves OIDs up to FirstNormalObjectId for manual and initdb
 * assignment (see transam.h). We can assume that any type created with an OID
 * starting at this number is a "custom" (user-created) type.
 */
static bool type_is_custom(Oid typeid) { return typeid >= FirstNormalObjectId; }

/*
 * Get and validate the attributes we send in the COPY command.
 *
 * With COPY, we send all the attributes except for GENERATED ones. Missing
 * attribute values will be encoded as NULL.
 *
 * Also check for possibility of using binary COPY mode. Binary mode is only
 * possible if all columns have a binary output (send) function and no columns
 * are arrays of user-defined (custom) types.
 *
 * The reason we cannot support arrays of user-defined types is because the
 * serialized format includes the array element's type Oid, which can differ
 * across databases when users create the type.
 */
static List *
get_insert_attrs(const Relation rel, bool *binary_possible)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	List *attrs = NIL;
	int i;

	Assert(binary_possible);
	*binary_possible = true;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		HeapTuple tup;
		Form_pg_type pt;

		if (attr->attisdropped || attr->attgenerated != '\0')
			continue;

		attrs = lappend_int(attrs, AttrOffsetGetAttrNumber(i));

		tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(attr->atttypid));

		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for type %u", attr->atttypid);

		pt = (Form_pg_type) GETSTRUCT(tup);

		if (OidIsValid(pt->typelem) && type_is_custom(pt->typelem))
			*binary_possible = false;

		if (!pt->typisdefined)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type %s is only a shell", format_type_be(attr->atttypid))));

		if (!OidIsValid(pt->typsend))
			*binary_possible = false;

		ReleaseSysCache(tup);
	}

	return attrs;
}

/*
 * Plan a remote INSERT using COPY on a hypertable.
 */
static List *
plan_remote_insert(PlannerInfo *root, DataNodeCopyPath *sdpath)
{
	ModifyTablePath *mtpath = sdpath->mtpath;
	RangeTblEntry *rte = planner_rt_fetch(sdpath->hypertable_rti, root);
	Relation rel;
	List *target_attrs = NIL;
	bool binary_possible = false;

	/* We cannot support ON CONFLICT with COPY, but this should be handled
	 * already so only assert. */
	Assert(mtpath->onconflict == NULL || mtpath->onconflict->action == ONCONFLICT_NONE);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);
	target_attrs = get_insert_attrs(rel, &binary_possible);
	table_close(rel, NoLock);

	return list_make3(target_attrs, makeInteger(mtpath->canSetTag), makeInteger(binary_possible));
}

static Plan *
data_node_copy_plan_create(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path,
						   List *tlist, List *clauses, List *custom_plans)
{
	DataNodeCopyPath *sdpath = (DataNodeCopyPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);
	Plan *subplan;

	Assert(list_length(custom_plans) == 1);

	subplan = linitial(custom_plans);
	cscan->methods = &data_node_copy_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = 0;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_scan_tlist = subplan->targetlist;
	cscan->custom_private = plan_remote_insert(root, sdpath);

	return &cscan->scan.plan;
}

static CustomPathMethods data_node_copy_path_methods = {
	.CustomName = "DataNodeCopyPath",
	.PlanCustomPath = data_node_copy_plan_create,
};

Path *
data_node_copy_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Index hypertable_rti,
						   int subplan_index)
{
	DataNodeCopyPath *sdpath = palloc0(sizeof(DataNodeCopyPath));
	Path *subpath = ts_chunk_dispatch_path_create(root, mtpath, hypertable_rti, subplan_index);

	/* Copy costs, etc. from the subpath */
	memcpy(&sdpath->cpath.path, subpath, sizeof(Path));

	sdpath->cpath.path.type = T_CustomPath;
	sdpath->cpath.path.pathtype = T_CustomScan;
	sdpath->cpath.custom_paths = list_make1(subpath);
	sdpath->cpath.methods = &data_node_copy_path_methods;
	sdpath->mtpath = mtpath;
	sdpath->hypertable_rti = hypertable_rti;
	sdpath->subplan_index = subplan_index;

	return &sdpath->cpath.path;
}

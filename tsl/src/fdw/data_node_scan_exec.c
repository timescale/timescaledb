/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/execnodes.h>
#include <nodes/relation.h>
#include <nodes/nodeFuncs.h>
#include <utils/memutils.h>
#include <utils/rel.h>

#include "scan_plan.h"
#include "scan_exec.h"
#include "data_node_scan_exec.h"

/*
 * The execution stage of a DataNodeScan.
 *
 * This implements the execution stage CustomScan interface for a DataNodeScan
 * plan. This is heavily based on the ForeignScan implementation, but allow
 * scans of remote relations that doesn't have a corresponding local foreign
 * table, which is the case for a data node relation.
 */

typedef struct DataNodeScanState
{
	CustomScanState css;
	TsFdwScanState fsstate;
	ExprState *recheck_quals;
	bool systemcol;
} DataNodeScanState;

static void
data_node_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	DataNodeScanState *sss = (DataNodeScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	List *fdw_exprs = linitial(cscan->custom_exprs);
	List *recheck_quals = lsecond(cscan->custom_exprs);
	List *fdw_private = list_nth(cscan->custom_private, 0);

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	fdw_scan_init(&node->ss, &sss->fsstate, cscan->custom_relids, fdw_private, fdw_exprs, eflags);

	sss->recheck_quals = ExecInitQual(recheck_quals, (PlanState *) node);
}

static TupleTableSlot *
data_node_scan_next(CustomScanState *node)
{
	DataNodeScanState *sss = (DataNodeScanState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext oldcontext;
	TupleTableSlot *slot;

	/* Call the Iterate function in short-lived context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	slot = fdw_scan_iterate(&node->ss, &sss->fsstate);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * If any system columns are requested, we have to force the tuple into
	 * physical-tuple form to avoid "cannot extract system attribute from
	 * virtual tuple" errors later.  We also insert a valid value for
	 * tableoid, which is the only actually-useful system column.
	 */
	if (sss->systemcol && !TupIsNull(slot))
	{
		HeapTuple tup = ExecMaterializeSlot(slot);

		tup->t_tableOid = RelationGetRelid(node->ss.ss_currentRelation);
	}

	return slot;
}

/*
 * Access method routine to recheck a tuple in EvalPlanQual
 */
static bool
data_node_scan_recheck(CustomScanState *node, TupleTableSlot *slot)
{
	DataNodeScanState *sss = (DataNodeScanState *) node;
	ExprContext *econtext;

	/*
	 * extract necessary information from the custom scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the remote qual condition? */
	econtext->ecxt_scantuple = slot;

	ResetExprContext(econtext);

	return ExecQual(sss->recheck_quals, econtext);
}

static TupleTableSlot *
data_node_scan_exec(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) data_node_scan_next,
					(ExecScanRecheckMtd) data_node_scan_recheck);
}

static void
data_node_scan_rescan(CustomScanState *node)
{
	fdw_scan_rescan(&node->ss, &((DataNodeScanState *) node)->fsstate);
}

static void
data_node_scan_end(CustomScanState *node)
{
	fdw_scan_end(&((DataNodeScanState *) node)->fsstate);
}

static void
data_node_scan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *scan = (CustomScan *) node->ss.ps.plan;
	List *fdw_private = list_nth(scan->custom_private, 0);

	fdw_scan_explain(&node->ss, fdw_private, es);
}

static CustomExecMethods data_node_scan_state_methods = {
	.CustomName = "DataNodeScanState",
	.BeginCustomScan = data_node_scan_begin,
	.EndCustomScan = data_node_scan_end,
	.ExecCustomScan = data_node_scan_exec,
	.ReScanCustomScan = data_node_scan_rescan,
	.ExplainCustomScan = data_node_scan_explain,
};

Node *
data_node_scan_state_create(CustomScan *cscan)
{
	DataNodeScanState *sss =
		(DataNodeScanState *) newNode(sizeof(DataNodeScanState), T_CustomScanState);

	sss->css.methods = &data_node_scan_state_methods;
	sss->systemcol = linitial_int(list_nth(cscan->custom_private, 1));

	return (Node *) sss;
}

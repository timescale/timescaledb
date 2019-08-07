/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/execnodes.h>
#include <nodes/nodeFuncs.h>
#include <executor/tuptable.h>
#include <utils/memutils.h>
#include <utils/rel.h>

#include "compat.h"

#if PG12_GE
#else
#include <nodes/relation.h>
#endif

#include "scan_plan.h"
#include "scan_exec.h"
#include "data_node_scan_exec.h"
#include "async_append.h"
#include "remote/cursor.h"

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
	AsyncScanState async_state;
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
#if PG12_LT
		HeapTuple tup = ExecMaterializeSlot(slot);

		tup->t_tableOid = RelationGetRelid(node->ss.ss_currentRelation);
#else
		slot->tts_tableOid = RelationGetRelid(node->ss.ss_currentRelation);
#endif
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

static void
send_create_cursor_req(AsyncScanState *ass)
{
	DataNodeScanState *dnss = (DataNodeScanState *) ass;
	create_cursor(&dnss->async_state.css.ss, &dnss->fsstate, false);
}

static void
complete_create_cursor(AsyncScanState *ass)
{
	DataNodeScanState *dnss = (DataNodeScanState *) ass;
	remote_cursor_wait_until_open(dnss->fsstate.cursor);
}

Node *
data_node_scan_state_create(CustomScan *cscan)
{
	DataNodeScanState *dnss =
		(DataNodeScanState *) newNode(sizeof(DataNodeScanState), T_CustomScanState);

	dnss->async_state.css.methods = &data_node_scan_state_methods;
	dnss->systemcol = linitial_int(list_nth(cscan->custom_private, 1));
	dnss->async_state.init = send_create_cursor_req;
	dnss->async_state.fetch_tuples = complete_create_cursor;
	return (Node *) dnss;
}
